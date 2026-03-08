package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_FileDoesNotExist(t *testing.T) {
	dir := t.TempDir()
	store := StateStore{path: filepath.Join(dir, "nonexistent.json")}

	state, err := store.Load()

	require.NoError(t, err)
	assert.Equal(t, 1, state.SchemaVersion)
	assert.NotNil(t, state.Records)
	assert.Empty(t, state.Records)
}

func TestLoad_CompletelyInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	os.WriteFile(path, []byte("not json at all {{{!!@#$"), 0644)

	store := StateStore{path: path}
	_, err := store.Load()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal state")
}

func TestLoad_TruncatedJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	os.WriteFile(path, []byte(`{"schemaVersion":1,"records":{"topic:0":{"firstSeen`), 0644)

	store := StateStore{path: path}
	_, err := store.Load()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal state")
}

func TestLoad_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	os.WriteFile(path, []byte(""), 0644)

	store := StateStore{path: path}
	_, err := store.Load()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal state")
}

func TestLoad_WrongJSONStructure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	os.WriteFile(path, []byte(`[1, 2, 3]`), 0644)

	store := StateStore{path: path}
	_, err := store.Load()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal state")
}

func TestLoad_ExtraUnknownFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	os.WriteFile(path, []byte(`{"schemaVersion":1,"records":{},"extra":"hi","anotherOne":42}`), 0644)

	store := StateStore{path: path}
	state, err := store.Load()

	require.NoError(t, err)
	assert.Equal(t, 1, state.SchemaVersion)
	assert.NotNil(t, state.Records)
	assert.Empty(t, state.Records)
}

func TestLoad_MissingFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	os.WriteFile(path, []byte(`{}`), 0644)

	store := StateStore{path: path}
	state, err := store.Load()

	require.NoError(t, err)
	assert.Equal(t, 1, state.SchemaVersion) // pre-initialized default survives empty JSON
	assert.NotNil(t, state.Records)
	assert.Empty(t, state.Records)
}

func TestLoad_NullRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	os.WriteFile(path, []byte(`{"schemaVersion":1,"records":null}`), 0644)

	store := StateStore{path: path}
	state, err := store.Load()

	require.NoError(t, err)
	assert.Equal(t, 1, state.SchemaVersion)
	assert.NotNil(t, state.Records)
	assert.Empty(t, state.Records)
}

func TestLoad_ValidRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store := StateStore{path: path}

	firstSeen := time.Now().Add(-2 * time.Hour).Truncate(time.Millisecond)
	original := PersistentState{
		SchemaVersion: 1,
		Records: map[string]IdleRecord{
			"topicA:0": {FirstSeenIdleAt: firstSeen, LastKnownOffset: 42},
			"topicB:1": {FirstSeenIdleAt: firstSeen, LastKnownOffset: 100},
		},
	}

	err := store.Save(original)
	require.NoError(t, err)

	loaded, err := store.Load()
	require.NoError(t, err)

	assert.Equal(t, original.SchemaVersion, loaded.SchemaVersion)
	assert.Len(t, loaded.Records, 2)

	recA := loaded.Records["topicA:0"]
	assert.True(t, firstSeen.Equal(recA.FirstSeenIdleAt), "FirstSeenIdleAt should survive round trip")
	assert.Equal(t, int64(42), recA.LastKnownOffset)

	recB := loaded.Records["topicB:1"]
	assert.True(t, firstSeen.Equal(recB.FirstSeenIdleAt), "FirstSeenIdleAt should survive round trip")
	assert.Equal(t, int64(100), recB.LastKnownOffset)
}
