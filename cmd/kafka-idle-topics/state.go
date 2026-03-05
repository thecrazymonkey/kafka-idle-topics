package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

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

// StateStore manages persistence of idle topic state to a JSON file.
type StateStore struct {
	path string
}

// Load returns the persisted state from disk.
// If the file does not exist, it returns an empty PersistentState (not an error).
// This is the correct first-run behaviour.
func (s *StateStore) Load() (PersistentState, error) {
	state := PersistentState{
		SchemaVersion: 1,
		Records:       make(map[string]IdleRecord),
	}

	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			// First run - no state file exists yet
			return state, nil
		}
		return state, fmt.Errorf("failed to read state file: %w", err)
	}

	if err := json.Unmarshal(data, &state); err != nil {
		return state, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	// Ensure Records is never nil
	if state.Records == nil {
		state.Records = make(map[string]IdleRecord)
	}

	return state, nil
}

// Save persists the current state to disk using a write-then-rename pattern
// to prevent corruption if the process is killed mid-write.
func (s *StateStore) Save(state PersistentState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	if err := os.Rename(tmp, s.path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("failed to commit state file: %w", err)
	}

	return nil
}

// pruneOrphanedRecords removes state records for topics that no longer exist in the cluster.
func pruneOrphanedRecords(state *PersistentState, knownTopics map[string][]int32) {
	for key := range state.Records {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) < 2 {
			// Invalid key format - remove it
			delete(state.Records, key)
			continue
		}
		topic := parts[0]
		if _, exists := knownTopics[topic]; !exists {
			delete(state.Records, key)
		}
	}
}
