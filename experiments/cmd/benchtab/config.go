package main

import (
	"flag"
	"log"
	"strings"

	"github.com/mmatczuk/scylla-go-driver"
)

type Workload int

const (
	Inserts Workload = iota
	Selects
	Mixed
)

type Config struct {
	nodeAddresses        []string
	workload             Workload
	user                 string
	password             string
	tasks                int64
	workers              int64
	batchSize            int64
	dontPrepare          bool
	async                bool
	profileCPU           bool
	profileMem           bool
	coalescingStrategy   int
	maxCoalesceWaitTime  int // in microseconds.
	maxCoalescedRequests int
}

func readConfig() Config {
	config := Config{}

	nodes := flag.String(
		"nodes",
		"",
		"Addresses of database nodes to connect to separated by a comma",
	)

	workload := flag.String(
		"workload",
		"mixed",
		"Type of work to perform (inserts, selects, mixed)",
	)

	flag.StringVar(
		&config.user,
		"user",
		"cassandra",
		"User",
	)

	flag.StringVar(
		&config.password,
		"password",
		"cassandra",
		"Password",
	)

	flag.Int64Var(
		&config.tasks,
		"tasks",
		1_000_000,
		"Total number of tasks (requests) to perform the during benchmark. In case of mixed workload there will be tasks inserts and tasks selects",
	)

	flag.Int64Var(
		&config.workers,
		"workers",
		1024,
		"Maximum number of workers",
	)

	flag.BoolVar(
		&config.dontPrepare,
		"dont-prepare",
		false,
		"Don't create tables and insert into them before the benchmark",
	)

	flag.BoolVar(
		&config.async,
		"async",
		false,
		"Use async query mode",
	)

	flag.BoolVar(
		&config.profileCPU,
		"profile-cpu",
		false,
		"Use CPU profiling",
	)

	flag.BoolVar(
		&config.profileMem,
		"profile-mem",
		false,
		"Use memory profiling",
	)

	flag.IntVar(
		&config.coalescingStrategy,
		"coalesce-strategy",
		scylla.MovingAverageStrategy,
		"Use coalescing strategy",
	)

	flag.IntVar(
		&config.maxCoalesceWaitTime,
		"waittime",
		1000,
		"Maximum pile-up wait in coalescing",
	)

	flag.IntVar(
		&config.maxCoalescedRequests,
		"max-coalesce",
		100,
		"Maximum number of frames to coalesce",
	)

	flag.Parse()

	for _, nodeAddress := range strings.Split(*nodes, ",") {
		config.nodeAddresses = append(config.nodeAddresses, nodeAddress)
	}

	switch *workload {
	case "inserts":
		config.workload = Inserts
	case "selects":
		config.workload = Selects
	case "mixed":
		config.workload = Mixed
	default:
		log.Fatal("invalid workload type")
	}

	config.batchSize = int64(256)

	max := func(a, b int64) int64 {
		if a > b {
			return a
		}

		return b
	}

	if config.tasks/config.batchSize < config.workers {
		config.batchSize = max(1, config.tasks/config.workers)
	}

	return config
}
