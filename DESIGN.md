# Design: Configurable Idle Period with Restart-Persistent Detection

## 1. Current State Analysis

### 1.1 Code Structure

The codebase is split across three files in `cmd/kafka-idle-topics/`:

| File | Contents |
|---|---|
| `kafka-idle-topics.go` | CLI flag parsing (`ReadCommands`), `main` entry point |
| `client.go` | `KafkaIdleTopics` struct, all filter methods, Kafka client creation |
| `helpers.go` | Utility functions, `StringArrayFlag` type, `Version` constant |

The Sarama library in use is `github.com/IBM/sarama` (ownership transferred from `github.com/Shopify/sarama`).

### 1.2 How Idle Detection Works Today

The tool identifies idle Kafka topics using several strategies controlled by the `idleMinutes` flag:

| Flag value | Strategy | Method |
|---|---|---|
| `0` (default) | `filterActiveProductionTopics` | Snapshot diff: records offsets, sleeps `productionAssessmentTimeMs`, re-reads offsets, marks unchanged topics idle |
| `> 0` | `filterTopicsIdleSince` | Message timestamp: consumes `OffsetNewest-1` per partition, compares the message timestamp to `now - idleMinutes` |

Two additional filters always run (unless skipped via `--skip`):
- `filterTopicsWithConsumerGroups` — marks topics as idle when no consumer group has a committed offset against them.
- `filterEmptyTopics` — marks topics as idle when oldest offset equals newest offset (no stored data).

Topic scoping is applied up front inside `getClusterTopics` before any filter runs:
- `--hideInternalTopics` — removes topics with a `_` prefix.
- `--hideTopicsPrefixes` — removes topics matching any of the provided prefixes.
- `--allowList` / `--disallowList` — restricts evaluation to or from an explicit topic list (via `filterListedTopics`).

Each filter method receives a **fresh** Kafka client (created via `getAdminClient` / `getClusterClient`) and closes it with `defer client.Close()` before returning.

### 1.3 Current Limitations

**Limitation 1 — No unified configurable idle window.**
`filterActiveProductionTopics` has a hard-coded observation window (`productionAssessmentTimeMs`, default 30 s). This measures *current* activity within a single run, not "has been idle for N minutes". There is no way to express "mark a topic idle only if it has had no production for the last 2 hours" using this code path.

**Limitation 2 — State is entirely in-memory.**
All detection state lives in `KafkaIdleTopics.DeleteCandidates` and in local offset snapshot variables. A restart discards everything. There is no persistent record of when a topic was first observed to be idle.

**Limitation 3 — `filterTopicsIdleSince` is partly restart-safe but brittle.**
It reads the stored Kafka message timestamp from `OffsetNewest-1`, which survives restarts for topics with at least one retained message. However:
- Topics with no messages time out after a hard-coded 5 seconds and are immediately marked idle with no configurable threshold.
- If messages have been deleted by retention policy, the last written message is no longer available, giving a false signal.

**Limitation 4 — Consumer group check does not consider staleness.**
`filterTopicsWithConsumerGroups` protects a topic from candidacy if any consumer group has a committed offset that is not `-1`. This is a blunt check: a consumer group that consumed a topic once six months ago and never ran again still has a committed offset, permanently protecting that topic regardless of actual idleness.

**Limitation 5 — Consumer group check does not account for temporarily stopped groups.**
A consumer group may be temporarily offline (e.g., during a deployment or maintenance window). Its membership state at the moment of a scan is transient and unreliable as an idleness signal. A consumer group that committed an offset 10 minutes ago but is currently stopped should still protect its topic.

---

## 2. Design Goals

1. **Single, unified idle window** — one flag, `--idleMinutes`, that means "a topic is idle if no relevant activity has occurred for at least N minutes", applied consistently across all detection strategies.
2. **Restart-persistent production tracking** — a topic's "first observed idle at" timestamp is written to disk so the configured window is measured correctly across application restarts.
3. **Accurate consumer group staleness detection** — a topic is only protected by a consumer group if that group has committed an offset within the idle window, regardless of whether the group is currently running.
4. **Minimal external dependencies** — use an embedded, file-based state store; no additional services required.
5. **Preserve all existing filtering capabilities** — `AllowList`, `DisallowList`, `hideDerivativeTopics`, `hideInternalTopics`, and `--skip` remain fully functional and unchanged.

---

## 3. Conceptual Model

A topic is reported as a **deletion candidate** only if all of the following are true (for non-skipped checks):

| Check | Idle condition |
|---|---|
| Production | No offset growth observed on any partition for at least `idleMinutes` (tracked persistently across restarts) |
| Consumption | No consumer group has committed an offset within the idle window on any partition, regardless of whether the group is currently running |
| Storage | All partitions have oldest offset equal to newest offset (completely empty) |

The idle window (`idleMinutes`) is the single configurable threshold that governs production and consumption checks uniformly. The storage check is binary (empty or not) and is not time-windowed.

---

## 4. Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                   kafka-idle-topics run                     │
│                                                             │
│  1. Load persistent state (StateStore.Load)                 │
│          │                                                  │
│  2. Fetch and scope topics (getClusterTopics):              │
│       apply AllowList, DisallowList, hideInternalTopics,    │
│       hideDerivativeTopics                                  │
│          │                                                  │
│  3. Prune state records for topics no longer in the cluster │
│          │                                                  │
│  4. assessProductionActivity (per topic/partition):         │
│       Has the partition offset grown since last run?        │
│          │                                                  │
│         YES → clear idle record, remove from candidates     │
│          NO  → create/update idle record with first-seen    │
│                timestamp; emit candidate if age >= window   │
│          │                                                  │
│  5. filterTopicsWithConsumerGroups:                         │
│       For each consumer group (active or stopped):          │
│         committedOffset >= OffsetForTimes(now-idleMinutes)? │
│          YES → topic is in use, remove from candidates      │
│          NO  → group is stale, does not protect             │
│          │                                                  │
│  6. filterEmptyTopics (unchanged)                           │
│          │                                                  │
│  7. filterOutDeleteCandidates                               │
│          │                                                  │
│  8. Persist updated state (StateStore.Save)                 │
│          │                                                  │
│  9. Write deletion candidates to output file                │
└─────────────────────────────────────────────────────────────┘
```

---

## 5. Component Design

### 5.1 Persistent State Store

A JSON file provides restart-safe state with zero additional dependencies.

#### Schema

```go
// IdleRecord captures when a partition was first observed idle.
type IdleRecord struct {
    FirstSeenIdleAt time.Time `json:"firstSeenIdleAt"`
    LastKnownOffset int64     `json:"lastKnownOffset"`
}

// PersistentState is the root object serialized to disk.
type PersistentState struct {
    SchemaVersion int                   `json:"schemaVersion"` // for future migrations
    Records       map[string]IdleRecord `json:"records"`       // key: "topic:partition"
}
```

The key format `"topic:partition"` (e.g., `"invoices:0"`) uniquely identifies each tracked partition.

#### Interface

```go
type StateStore struct {
    path string
}

func (s *StateStore) Load() (PersistentState, error)
// Load returns an empty PersistentState (not an error) when the file does not exist.
// This is the correct first-run behaviour.

func (s *StateStore) Save(state PersistentState) error
```

The state file path is configured via a new `--stateFile` flag, enabling container deployments to mount a persistent volume at a known path.

### 5.2 Production Activity Assessment

`filterActiveProductionTopics` and `filterTopicsIdleSince` are both removed and replaced by a single method:

```go
func (c *KafkaIdleTopics) assessProductionActivity(
    clusterClient sarama.Client,
    state *PersistentState,
) error
```

**Algorithm per topic/partition:**

1. Call `clusterClient.GetOffset(topic, partition, sarama.OffsetNewest)` — a lightweight metadata call, not a message fetch.
2. Look up the existing `IdleRecord` in `state.Records` for `"topic:partition"`.
3. **If the current offset is greater than `record.LastKnownOffset`** → the partition is actively receiving messages:
   - Delete the record from state (reset the idle clock).
   - Set `DeleteCandidates[topic] = false`.
4. **If the offset is unchanged:**
   - If no record exists → create one: `FirstSeenIdleAt = now`, `LastKnownOffset = currentOffset`.
   - If a record exists and `now - FirstSeenIdleAt >= idleMinutes` → mark as candidate.
   - If a record exists but the threshold is not yet reached → do nothing (continue watching).

**Why offset comparison instead of message consumption:**
- `GetOffset` with `sarama.OffsetNewest` is a Kafka metadata API call — no consumer required, no timeout risk.
- Works correctly for topics with zero messages (offset stays at `0`).
- Eliminates the per-partition goroutines, the 5-second hard-coded timeout, and the dependency on message retention for correctness.
- Produces a deterministic, restart-safe result when combined with the state file.

### 5.3 Consumer Group Staleness Check

`filterTopicsWithConsumerGroups` is updated to check whether each consumer group's committed offset falls within the idle window.

**The membership state of a consumer group (`DescribeConsumerGroups`) plays no role in this decision.** A group may be temporarily stopped due to a deployment, maintenance, or transient failure. Its stopped state is an operational condition, not evidence that the topic is idle. The committed offset timestamp is the durable, Kafka-stored evidence of recent use, and it must be evaluated regardless of whether the group is currently running.

**Algorithm:**

```
idleBoundaryTs = now - idleMinutes  (Unix milliseconds)

for each cg in ListConsumerGroups():
    offsets = ListConsumerGroupOffsets(cg, topicPartitionMap)

    for each (topic, partition, committedOffset) in offsets:

        if committedOffset == -1:
            # Group has never committed on this partition.
            # No protection — continue to next partition.
            continue

        idleBoundaryOffset = GetOffset(topic, partition, idleBoundaryTs)
        # GetOffset with a timestamp uses the OffsetForTimes API:
        # returns the earliest offset whose message timestamp >= idleBoundaryTs.

        if idleBoundaryOffset == -1:
            # OffsetForTimes found no messages at or after the boundary.
            # All retained data predates the idle window.
            # A committed offset into this range is stale → no protection.
            continue

        if committedOffset >= idleBoundaryOffset:
            # The group's last commit falls within the idle window.
            # Topic is in active use — remove from candidates.
            # This applies whether the group is currently running or stopped.
            DeleteCandidates[topic] = false
```

Note: `filterTopicsWithConsumerGroups` now requires both `adminClient` (for `ListConsumerGroups` and `ListConsumerGroupOffsets`) and `clusterClient` (for `GetOffset` with a timestamp). Both are created fresh by the caller and closed via `defer`.

**Edge cases:**

| Situation | `OffsetForTimes` result | Handling |
|---|---|---|
| All messages within the idle window deleted by retention | `-1` | Treat as stale — no recent data; committed offset does not protect |
| Partition is empty (never had messages) | `0` or `-1` | `committedOffset == -1` → never consumed → no protection |
| Topic has messages but none newer than idle boundary | `OffsetNewest` | `committedOffset >= OffsetNewest` means consumed up to head; topic is idle from a production perspective |
| Consumer group stopped temporarily, committed offset is recent | `idleBoundaryOffset` within range | `committedOffset >= idleBoundaryOffset` → topic is protected |
| Consumer group stopped months ago, committed offset is stale | `idleBoundaryOffset` > `committedOffset` | Topic is not protected — group is genuinely inactive |

### 5.4 Storage Check

`filterEmptyTopics` is unchanged. It compares `OffsetOldest` to `OffsetNewest` per partition using live Kafka metadata. This check is inherently restart-safe.

### 5.5 State Pruning

To prevent the state file from accumulating records for topics deleted from Kafka, orphaned records are pruned after `getClusterTopics` and before `StateStore.Save`:

```go
func pruneOrphanedRecords(state *PersistentState, knownTopics map[string][]int32) {
    for key := range state.Records {
        topic := strings.SplitN(key, ":", 2)[0]
        if _, exists := knownTopics[topic]; !exists {
            delete(state.Records, key)
        }
    }
}
```

---

## 6. Updated `KafkaIdleTopics` Struct

The following fields are added or removed relative to the current struct in `client.go`. All existing fields are preserved unchanged.

```go
type KafkaIdleTopics struct {
    kafkaUrl             string
    kafkaUsername        string
    kafkaPassword        string
    kafkaSecurity        string
    fileName             string
    stateFile            string          // NEW: path for the persistent state file
    skip                 string
    hideInternalTopics   bool
    AllowList            StringArrayFlag // unchanged
    DisallowList         StringArrayFlag // unchanged
    hideDerivativeTopics StringArrayFlag // unchanged
    idleMinutes          int64           // RENAMED from topicsIdleMinutes; now unified threshold

    // Removed: productionAssessmentTime (replaced by persistent idle window)
    // Removed: waitForTopicEvaluation (goroutine-per-topic approach eliminated)

    topicPartitionMap map[string][]int32
    DeleteCandidates  map[string]bool
}
```

---

## 7. Updated CLI Flags

Flags already present in the code are marked **existing**. Only `--idleMinutes` has a behaviour change; all others are preserved as-is.

| Flag | Default | Status | Description |
|---|---|---|---|
| `--bootstrap-servers` | `""` | Existing | Kafka cluster address(es). Also reads `KAFKA_BOOTSTRAP` env var. |
| `--username` | `""` | Existing | SASL username. Also reads `KAFKA_USERNAME` env var. |
| `--password` | `""` | Existing | SASL password. Also reads `KAFKA_PASSWORD` env var. |
| `--kafkaSecurity` | `"none"` | Existing | Connection security mode. |
| `--filename` | `"idleTopics.txt"` | Existing | Output filename. |
| `--skip` | `""` | Existing | Comma-delimited list of checks to skip: `production`, `consumption`, `storage`. |
| `--hideInternalTopics` | `false` | Existing | Exclude topics prefixed with `_`. |
| `--hideTopicsPrefixes` | `""` | Existing | Exclude topics matching any of the provided prefixes. |
| `--allowList` | `""` | Existing | Evaluate only the listed topics (file path or comma-delimited). |
| `--disallowList` | `""` | Existing | Exclude the listed topics from evaluation (file path or comma-delimited). |
| `--version` | `false` | Existing | Print version and exit. |
| `--idleMinutes` | `60` | **Default changed** from `0` | Minutes of inactivity before a topic is a deletion candidate. Also reads `KAFKA_IDLE_MINUTES` env var. Now governs both production and consumption checks uniformly. |
| `--stateFile` | `idle-state.json` | **New** | Path to the persistent idle-state file. Mount a persistent volume at this path in containerised deployments. |
| `--productionAssessmentTimeMs` | — | **Removed** | Replaced by the persistent idle window. |

> **Migration note:** users who relied on `productionAssessmentTimeMs` for the snapshot-diff approach should set `--idleMinutes` to a short value (e.g., `5`) and run the tool on a schedule. The idle clock starts on the first run and accumulates across subsequent runs.

---

## 8. Updated `main` Flow

Client creation follows the existing pattern: a **fresh client is created per call** and closed inside the called method via `defer`. This is preserved unchanged from the current code.

```go
func main() {
    myChecker := ReadCommands()
    // ... KAFKA_BOOTSTRAP / KAFKA_USERNAME / KAFKA_PASSWORD env var resolution (unchanged) ...
    // ... KAFKA_IDLE_MINUTES env var resolution (unchanged) ...

    store := StateStore{path: myChecker.stateFile}
    state, err := store.Load()
    if err != nil {
        log.Fatalf("Could not load state: %v", err)
    }

    // getClusterTopics internally applies AllowList, DisallowList,
    // hideInternalTopics, and hideDerivativeTopics filtering (unchanged).
    myChecker.topicPartitionMap = myChecker.getClusterTopics(
        myChecker.getAdminClient(myChecker.kafkaSecurity),
    )

    pruneOrphanedRecords(&state, myChecker.topicPartitionMap)

    stepsToSkip := strings.Split(myChecker.skip, ",")

    if !isInSlice("production", stepsToSkip) {
        myChecker.assessProductionActivity(
            myChecker.getClusterClient(myChecker.kafkaSecurity),
            &state,
        )
    }
    if !isInSlice("consumption", stepsToSkip) {
        myChecker.filterTopicsWithConsumerGroups(
            myChecker.getAdminClient(myChecker.kafkaSecurity),
            myChecker.getClusterClient(myChecker.kafkaSecurity),
        )
    }
    if !isInSlice("storage", stepsToSkip) {
        myChecker.filterEmptyTopics(myChecker.getClusterClient(myChecker.kafkaSecurity))
    }

    myChecker.filterOutDeleteCandidates()

    if err := store.Save(state); err != nil {
        log.Printf("WARN: Could not persist state: %v", err)
    }

    myChecker.writeDeleteCandidatesLocally()
}
```

---

## 9. Restart-Survival Scenarios

| Scenario | Behaviour |
|---|---|
| First run (no state file) | All topics start fresh. Idle partitions get `FirstSeenIdleAt = now`. No topics are emitted as candidates on the first run (unless `idleMinutes = 0`). |
| Subsequent run, topic still idle | `now - FirstSeenIdleAt` is checked against the threshold. Topics crossing it are emitted. |
| Subsequent run, topic becomes active | Offset grew → record deleted from state → topic exits candidacy. |
| Application restarted mid-window | State file preserves `FirstSeenIdleAt`. The idle clock continues from where it left off, not from zero. |
| State file deleted between runs | Equivalent to a first run. Idle clocks reset. Safe — conservative by design. |
| Topic deleted from Kafka | Pruning removes its state record. No stale entries accumulate. |
| Topic scoped out by AllowList/DisallowList | It no longer appears in `topicPartitionMap`. Pruning removes its state record on the next save. |
| Consumer group temporarily stopped | Committed offset recency check still applies. If the group committed within `idleMinutes`, the topic is protected regardless of group membership state. |
| Consumer group permanently abandoned | Committed offset predates the idle window → `committedOffset < idleBoundaryOffset` → does not protect. |
| `--idleMinutes` increased between runs | Existing `FirstSeenIdleAt` timestamps remain valid; topics may need to wait longer before being emitted. |
| `--idleMinutes` decreased between runs | Topics already in state whose age now exceeds the new shorter threshold are emitted on the next run. |

---

## 10. File Layout

```
cmd/kafka-idle-topics/
├── kafka-idle-topics.go                   # flag parsing and main (updated)
├── client.go                              # KafkaIdleTopics struct and filter methods (updated)
├── helpers.go                             # utility functions, StringArrayFlag (unchanged)
├── state.go                               # NEW: StateStore, PersistentState, IdleRecord,
│                                          #      pruneOrphanedRecords
└── kafka-idle-topics-Integration_test.go  # updated tests
```

`state.go` is kept separate to allow independent unit testing without Kafka client dependencies.

---

## 11. Testing Strategy

| Test type | Coverage |
|---|---|
| Unit: `StateStore` | Load from missing file returns empty state; Save/Load round-trip preserves all fields. |
| Unit: `assessProductionActivity` | Growing offset clears existing record; stable offset creates a record on first call; stable offset exceeding threshold adds to `DeleteCandidates`; stable offset below threshold does nothing. |
| Unit: `filterTopicsWithConsumerGroups` | `committedOffset == -1` → no protection; `committedOffset >= idleBoundaryOffset` → protects topic; `committedOffset < idleBoundaryOffset` → does not protect; stopped group with recent commit → protects; stopped group with stale commit → does not protect. |
| Unit: `pruneOrphanedRecords` | Records for unknown topics removed; records for known topics preserved. |
| Integration (existing harness) | Pass a temp state file path in `setup()`. Add a test that simulates two sequential runs: first run creates state records; second run (with mocked or real elapsed time exceeding `idleMinutes`) emits the expected candidates. |

---

## 12. Summary of Changes to Current Code

| Item | Current behaviour | New behaviour |
|---|---|---|
| `filterActiveProductionTopics` | Snapshot diff within a single run; state lost on restart | Removed — replaced by `assessProductionActivity` |
| `filterTopicsIdleSince` | Message timestamp from consumed record; goroutines with hard-coded 5 s timeout | Removed — replaced by `assessProductionActivity` |
| `assessProductionActivity` | Does not exist | New: offset comparison per partition, persistent idle clock via `StateStore` |
| `filterTopicsWithConsumerGroups` | Protects topic if any group has `Offset != -1`, regardless of when it committed | Updated: protects topic only if any group's committed offset >= `OffsetForTimes(now - idleMinutes)`; group membership state is not considered; now accepts a second `clusterClient` parameter |
| `filterEmptyTopics` | Unchanged | Unchanged |
| `filterListedTopics` | Unchanged | Unchanged |
| `getClusterTopics` | Unchanged | Unchanged |
| `StateStore` | Does not exist | New: JSON file at `--stateFile` path; loaded at startup, saved before exit |
| `StringArrayFlag`, `AllowList`, `DisallowList`, `hideDerivativeTopics` | Unchanged | Unchanged |
| `--productionAssessmentTimeMs` flag | 30000 ms default | Removed |
| `--idleMinutes` flag | `0` default; selects between two code paths | `60` default; single unified threshold for production and consumption checks |
| `--stateFile` flag | Does not exist | New: path for persistent state file |
| Sarama import | `github.com/IBM/sarama` | Unchanged |
