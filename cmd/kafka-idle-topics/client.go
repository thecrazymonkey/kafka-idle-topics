package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

type KafkaIdleTopics struct {
	kafkaUrl             string
	kafkaUsername        string
	kafkaPassword        string
	kafkaSecurity        string
	fileName             string
	stateFile            string // path for the persistent state file
	skip                 string
	hideInternalTopics   bool
	AllowList            StringArrayFlag
	DisallowList         StringArrayFlag
	hideDerivativeTopics StringArrayFlag
	idleMinutes          int64 // unified threshold for production and consumption checks
	topicPartitionMap    map[string][]int32
	DeleteCandidates     map[string]bool
}

/*
Uses the provided Sarama Admin Client to get a list of current topics in the cluster
*/
func (c *KafkaIdleTopics) getClusterTopics(adminClient sarama.ClusterAdmin) map[string][]int32 {
	defer adminClient.Close()
	log.Println("Loading Topics...")
	topicMetadata, err := adminClient.ListTopics()
	if err != nil {
		log.Fatalf("Could not reach cluster within the last 30 seconds. Is the configuration correct? %v", err)
	}

	c.topicPartitionMap = map[string][]int32{}
	for t, td := range topicMetadata {
		c.topicPartitionMap[t] = makeRange(0, td.NumPartitions-1)
	}

	c.filterListedTopics(c.topicPartitionMap)

	if c.hideInternalTopics {
		for t := range c.topicPartitionMap {
			if strings.HasPrefix(t, "_") {
				delete(c.topicPartitionMap, t)
			}
		}
	}

	if c.hideDerivativeTopics != nil {
		for t := range c.topicPartitionMap {
			for candidate := range c.hideDerivativeTopics {
				if strings.HasPrefix(t, candidate) {
					delete(c.topicPartitionMap, t)
				}
			}
		}
	}

	return c.topicPartitionMap
}

// markDeleteCandidate marks topic as a deletion candidate unless it is already
// protected (DeleteCandidates[topic] == false).
func (c *KafkaIdleTopics) markDeleteCandidate(topic string) {
	if v, exists := c.DeleteCandidates[topic]; !exists || v {
		c.DeleteCandidates[topic] = true
	}
}

// markDeleteCandidateIfIdle marks a topic as a deletion candidate only if all
// of its partitions have been tracked as idle in the persistent state for at
// least c.idleMinutes. If any partition lacks a state record, one is created
// with the current time so the idle clock starts ticking.
func (c *KafkaIdleTopics) markDeleteCandidateIfIdle(topic string, state *PersistentState, now time.Time) {
	idleThreshold := time.Duration(c.idleMinutes) * time.Minute
	partitions := c.topicPartitionMap[topic]
	allIdleLongEnough := true

	for _, partition := range partitions {
		key := fmt.Sprintf("%s:%d", topic, partition)
		record, exists := state.Records[key]
		if !exists {
			state.Records[key] = IdleRecord{
				FirstSeenIdleAt: now,
				LastKnownOffset: -1,
			}
			if c.idleMinutes > 0 {
				allIdleLongEnough = false
			}
			continue
		}
		if now.Sub(record.FirstSeenIdleAt) < idleThreshold {
			allIdleLongEnough = false
		}
	}

	if allIdleLongEnough {
		c.markDeleteCandidate(topic)
	}
}

/*
Assesses production activity for all topics by comparing current offsets to previously recorded offsets.
Updates the persistent state and marks topics as deletion candidates if they have been idle
for longer than c.idleMinutes.
*/
func (c *KafkaIdleTopics) assessProductionActivity(
	clusterClient sarama.Client,
	state *PersistentState,
) error {
	defer clusterClient.Close()
	log.Println("Evaluating production activity across all topics...")

	now := time.Now()
	idleThreshold := time.Duration(c.idleMinutes) * time.Minute

	for topic, partitions := range c.topicPartitionMap {
		topicHasActivity := false

		for _, partition := range partitions {
			// Get current offset for this partition
			currentOffset, err := clusterClient.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				log.Printf("WARN: Could not determine offset for %s:%d: %v", topic, partition, err)
				continue
			}

			key := fmt.Sprintf("%s:%d", topic, partition)
			record, exists := state.Records[key]

			if !exists {
				// First time seeing this partition - create a record
				state.Records[key] = IdleRecord{
					FirstSeenIdleAt: now,
					LastKnownOffset: currentOffset,
				}
				// With idleMinutes=0 the threshold is already met on first sighting
				if c.idleMinutes == 0 {
					c.markDeleteCandidate(topic)
				}
				continue
			}

			if currentOffset > record.LastKnownOffset {
				// Partition is actively receiving messages - reset idle tracking
				delete(state.Records, key)
				topicHasActivity = true
			} else {
				// Offset unchanged - check if idle threshold exceeded
				idleDuration := now.Sub(record.FirstSeenIdleAt)
				if idleDuration >= idleThreshold {
					// Topic has been idle long enough - mark as candidate
					c.markDeleteCandidate(topic)
				}
				// Update the record with current offset (in case it matters for future runs)
				state.Records[key] = IdleRecord{
					FirstSeenIdleAt: record.FirstSeenIdleAt,
					LastKnownOffset: currentOffset,
				}
			}
		}

		// If any partition showed activity, mark the topic as not a candidate
		if topicHasActivity {
			c.DeleteCandidates[topic] = false
		}
	}

	return nil
}

/*
Adds topics with nothing stored in them to c.DeleteCandidates
It is also possible for this method to remove candidacy if it detects activity.
*/
func (c *KafkaIdleTopics) filterEmptyTopics(clusterClient sarama.Client, state *PersistentState) {
	defer clusterClient.Close()
	log.Println("Evaluating Topics without anything in them...")

	now := time.Now()

	for topic, td := range c.topicPartitionMap {
		thisTopicsPartitions := td
		for _, partition := range thisTopicsPartitions {
			oldestOffsetForPartition, err := clusterClient.GetOffset(topic, partition, sarama.OffsetOldest)
			if err != nil {
				log.Fatalf("Could not determine topic storage: %v", err)
			}
			newestOffsetForPartition, err := clusterClient.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				log.Fatalf("Could not determine topic storage: %v", err)
			}
			if oldestOffsetForPartition != newestOffsetForPartition {
				c.DeleteCandidates[topic] = false
				break
			} else {
				c.markDeleteCandidateIfIdle(topic, state, now)
			}
		}
	}
}

/*
Evaluates consumer group staleness and removes topics from deletion candidates
if any consumer group has committed an offset within the idle window.
This check applies regardless of whether the consumer group is currently running.
*/
func (c *KafkaIdleTopics) filterTopicsWithConsumerGroups(
	adminClient sarama.ClusterAdmin,
	clusterClient sarama.Client,
	state *PersistentState,
) {
	defer adminClient.Close()
	defer clusterClient.Close()
	log.Println("Evaluating Topics with consumer group activity...")

	now := time.Now()

	allConsumerGroups, err := adminClient.ListConsumerGroups()
	if err != nil {
		log.Fatalf("Could not obtain Consumer Groups from cluster: %v", err)
	}

	// Calculate the idle boundary timestamp (in milliseconds)
	idleBoundaryTs := now.Add(-time.Duration(c.idleMinutes) * time.Minute).UnixMilli()

	// Pre-fetch boundary offsets once per partition to avoid redundant Kafka API
	// calls inside the per-consumer-group loop below.
	boundaryOffsets := make(map[string]int64)
	for topic, partitions := range c.topicPartitionMap {
		for _, partition := range partitions {
			offset, err := clusterClient.GetOffset(topic, partition, idleBoundaryTs)
			if err != nil {
				log.Printf("WARN: Could not get offset for timestamp on %s:%d: %v", topic, partition, err)
				continue
			}
			boundaryOffsets[fmt.Sprintf("%s:%d", topic, partition)] = offset
		}
	}

	for cg := range allConsumerGroups {
		result, err := adminClient.ListConsumerGroupOffsets(cg, c.topicPartitionMap)
		if err != nil {
			log.Printf("WARN: Cannot determine offsets for consumer group %s: %v", cg, err)
			continue
		}

		for topic, partitionData := range result.Blocks {
			// Skip topics already confirmed as in-use by a previous group
			if v, exists := c.DeleteCandidates[topic]; exists && !v {
				continue
			}
			for partition, dataset := range partitionData {
				committedOffset := dataset.Offset

				// Group has never committed on this partition - no protection
				if committedOffset == -1 {
					c.markDeleteCandidateIfIdle(topic, state, now)
					continue
				}

				idleBoundaryOffset, ok := boundaryOffsets[fmt.Sprintf("%s:%d", topic, partition)]
				if !ok {
					// Error already logged during pre-fetch; skip this partition
					continue
				}

				// idleBoundaryOffset == -1 means no messages at or after the boundary exist
				// All retained data predates the idle window - committed offset is stale
				if idleBoundaryOffset == -1 {
					c.markDeleteCandidateIfIdle(topic, state, now)
					continue
				}

				// If committed offset >= idleBoundaryOffset, the group consumed within the window
				// Topic is in active use - protect it regardless of group's current running state
				if committedOffset >= idleBoundaryOffset {
					c.DeleteCandidates[topic] = false
					break // One active group is enough to protect the topic
				} else {
					// Committed offset is stale (predates the idle window)
					c.markDeleteCandidateIfIdle(topic, state, now)
				}
			}
		}
	}
}

/*
Filters c.topicPartitionMap to include the same topics as c.DeleteCandidates
and cleans c.DeleteCandidates to only include topics to be removed.
*/
func (c *KafkaIdleTopics) filterOutDeleteCandidates() {

	for t := range c.topicPartitionMap {
		v, existsInCandidates := c.DeleteCandidates[t]
		if existsInCandidates && !v {
			delete(c.topicPartitionMap, t)
			delete(c.DeleteCandidates, t)
		} else if !existsInCandidates {
			delete(c.topicPartitionMap, t)
		}
	}
}

func (c *KafkaIdleTopics) getAdminClient(securityContext string) sarama.ClusterAdmin {
	adminClient, err := sarama.NewClusterAdmin(strings.Split(c.kafkaUrl, ","), c.generateClientConfigs(securityContext))
	if err != nil {
		log.Fatalf("Unable to create Kafka Client: %v", err)
	}
	return adminClient
}

func (c *KafkaIdleTopics) getClusterClient(securityContext string) sarama.Client {
	clusterClient, err := sarama.NewClient(strings.Split(c.kafkaUrl, ","), c.generateClientConfigs(securityContext))
	if err != nil {
		log.Fatalf("Unable to create Kafka Client: %v", err)
	}
	return clusterClient
}

func (c *KafkaIdleTopics) generateClientConfigs(securityContext string) *sarama.Config {
	clientConfigs := sarama.NewConfig()
	clientConfigs.ClientID = "kafka-idle-topics"
	clientConfigs.Producer.Return.Successes = true
	clientConfigs.Consumer.Return.Errors = true
	clientConfigs.Consumer.Offsets.AutoCommit.Enable = true
	clientConfigs.Consumer.Offsets.AutoCommit.Interval = time.Duration(10) * time.Millisecond
	if securityContext == "plain_tls" {
		clientConfigs.Net.SASL.Enable = true
		clientConfigs.Net.SASL.User = c.kafkaUsername
		clientConfigs.Net.SASL.Password = c.kafkaPassword
		clientConfigs.Net.TLS.Enable = true
	} else if securityContext == "plain" {
		clientConfigs.Net.SASL.Enable = true
		clientConfigs.Net.SASL.User = c.kafkaUsername
		clientConfigs.Net.SASL.Password = c.kafkaPassword
	} else if securityContext == "tls" {
		clientConfigs.Net.TLS.Enable = true
	}
	return clientConfigs
}

func (c *KafkaIdleTopics) writeDeleteCandidatesLocally() string {
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Could not write results due to: %v", err)
	}

	file, err := os.Create(fmt.Sprintf("%s/%s", currentDir, c.fileName))
	if err != nil {
		log.Fatalf("Could not write results due to: %v", err)
	}
	defer file.Close()

	for topic := range c.DeleteCandidates {
		_, err := file.WriteString(topic)
		if err != nil {
			log.Printf("WARN: Could not write this topic to file: %s", topic)
		}
		_, err = file.WriteString("\n")
		if err != nil {
			log.Printf("WARN: Could not write this topic to file: %s", topic)
		}
	}
	file.Sync()
	return file.Name()
}

// Filters the provided slice of topics according to what is provided in AllowList and DisallowList
func (c *KafkaIdleTopics) filterListedTopics(response map[string][]int32) {

	for s, _ := range response { // Filter out for lists
		if c.AllowList != nil { // If allow list is defined
			_, allowContains := c.AllowList[s]
			if !allowContains { // If allow list does not contain it, delete it
				delete(response, s)
			}
		}
		if c.DisallowList != nil { // If disallow list is defined
			_, disallowContains := c.DisallowList[s]
			if disallowContains { // If disallow list contains it, delete it
				delete(response, s)
			}
		}
	}
}
