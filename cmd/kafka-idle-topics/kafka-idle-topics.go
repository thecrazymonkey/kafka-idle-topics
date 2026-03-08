package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func NewKafkaIdleTopics() *KafkaIdleTopics {
	thisInstance := KafkaIdleTopics{}
	thisInstance.DeleteCandidates = make(map[string]bool)
	return &thisInstance
}

func ReadCommands() *KafkaIdleTopics {
	thisInstance := NewKafkaIdleTopics()

	flag.StringVar(&thisInstance.kafkaUrl, "bootstrap-servers", "", "Address to the target Kafka Cluster. Accepts multiple endpoints separated by a comma. Can be set using env variable KAFKA_BOOTSTRAP")
	flag.StringVar(&thisInstance.kafkaUsername, "username", "", "Username in the PLAIN module. Can be set using env variable KAFKA_USERNAME")
	flag.StringVar(&thisInstance.kafkaPassword, "password", "", "Password in the PLAIN module. Can be set using env variable KAFKA_PASSWORD")
	flag.StringVar(&thisInstance.kafkaSecurity, "kafkaSecurity", "none", "Type of connection to attempt. Options: plain_tls, plain (no tls), tls (one-way), none.")
	flag.StringVar(&thisInstance.fileName, "filename", "idleTopics.txt", "Custom filename for the output if needed.")
	flag.StringVar(&thisInstance.stateFile, "stateFile", "idle-state.json", "Path to the persistent idle-state file. Mount a persistent volume at this path in containerised deployments.")
	flag.StringVar(&thisInstance.skip, "skip", "", "Filtering to skip. Options are: production, consumption, storage. This can be a comma-delimited list.")
	flag.Int64Var(&thisInstance.idleMinutes, "idleMinutes", 60, "Minutes of inactivity before a topic is a deletion candidate. Governs both production and consumption checks uniformly. Can be set using env variable KAFKA_IDLE_MINUTES")
	flag.BoolVar(&thisInstance.hideInternalTopics, "hideInternalTopics", false, "Hide internal topics from assessment.")
	flag.Var(&thisInstance.hideDerivativeTopics, "hideTopicsPrefixes", "Disqualify provided prefixes from assessment. A comma delimited list. It also accepts a path to a file containing a list.")
	flag.Var(&thisInstance.AllowList, "allowList", "A comma delimited list of topics to evaluate. It also accepts a path to a file containing a list of topics.")
	flag.Var(&thisInstance.DisallowList, "disallowList", "A comma delimited list of topics to exclude from evaluation. It also accepts a path to a file containing a list of topics.")
	versionFlag := flag.Bool("version", false, "Print the current version and exit")

	flag.Parse()

	if *versionFlag {
		fmt.Printf("kafka-idle-topics: %s\n", Version)
		os.Exit(0)
	}

	return thisInstance
}

func main() {

	myChecker := ReadCommands()

	// KAFKA_BOOTSTRAP applies to all security modes
	if myChecker.kafkaUrl == "" {
		myChecker.kafkaUrl, _ = GetOSEnvVar("KAFKA_BOOTSTRAP")
	}
	if myChecker.kafkaSecurity == "plain_tls" || myChecker.kafkaSecurity == "plain" {
		if myChecker.kafkaUsername == "" {
			myChecker.kafkaUsername, _ = GetOSEnvVar("KAFKA_USERNAME")
		}
		if myChecker.kafkaPassword == "" {
			myChecker.kafkaPassword, _ = GetOSEnvVar("KAFKA_PASSWORD")
		}
	}

	// Allow env var to override idleMinutes only if the flag was not explicitly set
	idleMinutesExplicitlySet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "idleMinutes" {
			idleMinutesExplicitlySet = true
		}
	})
	if !idleMinutesExplicitlySet {
		envVar, err := GetOSEnvVar("KAFKA_IDLE_MINUTES")
		if err == nil {
			idleInt, err := strconv.ParseInt(envVar, 10, 64)
			if err != nil {
				log.Printf("Couldn't parse env var %v, using default of 60", err)
			} else {
				myChecker.idleMinutes = idleInt
			}
		}
	}

	// Load persistent state
	store := StateStore{path: myChecker.stateFile}
	state, err := store.Load()
	if err != nil {
		log.Fatalf("Could not load state: %v", err)
	}

	stepsToSkip := strings.Split(myChecker.skip, ",")

	// Extract Topics in Cluster (applies AllowList, DisallowList, hideInternalTopics, hideDerivativeTopics)
	myChecker.topicPartitionMap = myChecker.getClusterTopics(myChecker.getAdminClient(myChecker.kafkaSecurity))

	// Prune state records for topics no longer in the cluster
	pruneOrphanedRecords(&state, myChecker.topicPartitionMap)

	if !isInSlice("production", stepsToSkip) {
		err := myChecker.assessProductionActivity(
			myChecker.getClusterClient(myChecker.kafkaSecurity),
			&state,
		)
		if err != nil {
			log.Fatalf("Could not assess production activity: %v", err)
		}
	}

	if !isInSlice("consumption", stepsToSkip) {
		myChecker.filterTopicsWithConsumerGroups(
			myChecker.getAdminClient(myChecker.kafkaSecurity),
			myChecker.getClusterClient(myChecker.kafkaSecurity),
			&state,
		)
	}

	if !isInSlice("storage", stepsToSkip) {
		myChecker.filterEmptyTopics(myChecker.getClusterClient(myChecker.kafkaSecurity), &state)
	}

	myChecker.filterOutDeleteCandidates()

	// Persist updated state
	if err := store.Save(state); err != nil {
		log.Printf("WARN: Could not persist state: %v", err)
	}

	path := myChecker.writeDeleteCandidatesLocally()

	partitionCount := 0
	for _, ps := range myChecker.topicPartitionMap {
		partitionCount = partitionCount + len(ps)
	}

	log.Printf("Done! You can delete %v topics with %v partitions! A list of found idle topics is available at: %s", len(myChecker.topicPartitionMap), partitionCount, path)
}
