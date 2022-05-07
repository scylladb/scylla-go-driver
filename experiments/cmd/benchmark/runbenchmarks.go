package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
)

var addr = "192.168.100.100:9042"
var runs = 5
var workloads = []string{"inserts", "mixed"}
var tasks = []int{1_000_000, 10_000_000, 100_000_000}
var workers = []int{512, 1024, 2048, 4096, 8192}
var cpu = runtime.NumCPU()
var asyncWorkers = []int{cpu, cpu * 2, cpu * 4, cpu * 8, cpu * 16}
var batchSize = []int{256, 256 * 2, 256 * 4, 256 * 8, 256 * 16}
var defaultBatchSize = 256

type benchResult struct {
	name      string
	workload  string
	tasks     int
	workers   int
	batchSize int
	time      []int
	mean      float64
	dev       float64
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func newBenchResult(name, workload string, runs, tasks, workers, batch int) benchResult {
	if tasks/batch < workers {
		batch = max(1, tasks/workers)
	}

	return benchResult{
		name:      name,
		workload:  workload,
		tasks:     tasks,
		workers:   workers,
		batchSize: batch,
		time:      make([]int, runs),
	}
}

func (r *benchResult) insert(time, pos int) {
	r.time[pos] = time
}

func (r *benchResult) calculateMeanAndDev() {
	sum := 0
	for _, t := range r.time {
		sum += t
	}
	r.mean = float64(sum / len(r.time))

	sq := float64(0)
	for _, t := range r.time {
		sq += (float64(t) - r.mean) * (float64(t) - r.mean)
	}

	r.dev = math.Sqrt((sq / float64(len(r.time))))
}

func getTime(input string) int {
	reg, err := regexp.Compile("Benchmark Time: ([0-9]+) ms")
	if err != nil {
		panic(err)
	}

	time, err := strconv.Atoi(reg.FindStringSubmatch(input)[1])
	if err != nil {
		panic(err)
	}

	return time
}

func addFlags(cmd, workload, addr string, tasks, workers int) string {
	return cmd + " -nodes " + addr + " -workload " + workload + " -tasks " + strconv.Itoa(tasks) + " -workers " + strconv.Itoa(workers)
}

func runBenchmark(name, cmd, path string) []benchResult {
	var results []benchResult
	for _, workload := range workloads {
		for _, tasksNum := range tasks {
			for _, workersNum := range workers {
				result := newBenchResult(name, workload, runs, tasksNum, workersNum, defaultBatchSize)
				cmdWithFlags := addFlags(cmd, workload, addr, tasksNum, workersNum)
				for i := 0; i < runs; i++ {
					log.Printf("%s - run: %v, workload: %s, tasks: %v, workers: %v, batch: %v", name, i+1, workload, tasksNum, workersNum, result.batchSize)
					out, err := exec.Command("/bin/sh", "-c", "cd "+path+"; "+cmdWithFlags+";").Output()
					if err != nil {
						panic(err)
					}

					time := getTime(string(out))
					log.Printf(" time: %v\n", time)
					result.insert(time, i)
				}
				result.calculateMeanAndDev()
				results = append(results, result)
			}
		}
	}

	return results
}

func runAsyncBenchmark(name, cmd, path string) []benchResult {
	var results []benchResult
	for _, workload := range workloads {
		for _, tasksNum := range tasks {
			for _, workersNum := range asyncWorkers {
				for _, batch := range batchSize {
					result := newBenchResult(name, workload, runs, tasksNum, workersNum, batch)
					cmdWithFlags := addFlags(cmd, workload, addr, tasksNum, workersNum)
					cmdWithFlags += " -batch-size " + strconv.Itoa(result.batchSize)
					for i := 0; i < runs; i++ {
						log.Printf("%s - run: %v, workload: %s, tasks: %v, workers: %v batch: %v", name, i+1, workload, tasksNum, workersNum, result.batchSize)
						out, err := exec.Command("/bin/sh", "-c", "cd "+path+"; "+cmdWithFlags+";").Output()
						if err != nil {
							panic(err)
						}
						time := getTime(string(out))
						log.Printf(" time: %v\n", time)
						result.insert(time, i)
					}
					result.calculateMeanAndDev()
					results = append(results, result)
				}
			}
		}
	}
	return results
}

func makeCSV(out string, results []benchResult) {
	csvFile, err := os.Create(out + ".csv")
	if err != nil {
		panic(csvFile)
	}
	csvWriter := csv.NewWriter(csvFile)

	head := []string{"Driver", "Workload", "Tasks", "Workers", "Batch Size", "Time", "Standard Deviation"}
	csvWriter.Write(head)

	for _, result := range results {
		row := []string{
			result.name,
			result.workload,
			strconv.Itoa(result.tasks),
			strconv.Itoa(result.workers),
			strconv.Itoa(result.batchSize),
			fmt.Sprintf("%f", result.mean),
			fmt.Sprintf("%f", result.dev),
		}

		csvWriter.Write(row)
	}

	csvWriter.Flush()
	csvFile.Close()
}

func main() {
	scyllaGo := ""
	gocql := ""
	scyllaRust := ""

	scyllaGoResults := runBenchmark("scylla-go-driver", "go run", scyllaGo)
	scyllaRustResults := runBenchmark("scylla-rust-driver", "cargo run --release", scyllaRust)
	gocqlResults := runBenchmark("gocql", "go run", gocql)
	scyllaGoAsyncResults := runAsyncBenchmark("scylla-go-driver async", "go run", scyllaGo)

	var results []benchResult
	results = append(results, scyllaGoResults...)
	results = append(results, scyllaGoAsyncResults...)
	results = append(results, scyllaRustResults...)
	results = append(results, gocqlResults...)

	makeCSV("./benchmarkResults", results)
}
