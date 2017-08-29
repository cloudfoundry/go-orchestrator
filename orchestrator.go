// Package orchestrate an algorithm that manages the work of a cluster of
// nodes. It ensures each piece of work has a worker assigned to it.
//
// The Orchestrator stores a set of expected tasks. Each term, it reaches out
// to the cluster to gather what each node is working on. These tasks are
// called the actual tasks. The Orchestrator adjusts the nodes workload to
// attempt to match the expected tasks.
//
// The expected workload is stored in memory. Therefore, if the process is
// restarted the task list is lost. A system with persistence is required to
// ensure the workload is not lost (e.g., database).
package orchestrate

import (
	"context"
	"io/ioutil"
	"log"
	"sync"
)

// Orchestrator stores the expected workload and reaches out to the cluster
// to see what the actual workload is. It then tries to fix the delta.
//
// The expected task list can be altered via AddTask, RemoveTask and
// UpdateTasks. Each method is safe to be called on multiple go-routines.
type Orchestrator struct {
	log Logger
	c   Communicator

	mu            sync.Mutex
	workers       []string
	expectedTasks []string
}

// New creates a new Orchestrator.
func New(c Communicator, opts ...OrchestratorOption) *Orchestrator {
	o := &Orchestrator{
		c:   c,
		log: log.New(ioutil.Discard, "", 0),
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}

// Communicator manages the intra communication between the Orchestrator and
// the node cluster. Each method must be safe to call on many go-routines.
type Communicator interface {
	// List returns the workload from the given worker.
	List(ctx context.Context, worker string) ([]string, error)

	// Add adds the given task to the worker.
	Add(ctx context.Context, worker, task string) error

	// Removes the given task from the worker.
	Remove(ctx context.Context, worker, task string) error
}

//OrchestratorOption configures an Orchestrator.
type OrchestratorOption func(*Orchestrator)

type Logger interface {
	Printf(format string, v ...interface{})
}

// WithLogger sets the logger for the Orchestrator.
func WithLogger(l Logger) OrchestratorOption {
	return func(o *Orchestrator) {
		o.log = l
	}
}

// NextTerm reaches out to the cluster to gather to actual workload. It then
// attempts to fix the delta between actual and expected. The lifecycle of
// the term is managed by the given context.
func (o *Orchestrator) NextTerm(ctx context.Context) {
	o.log.Printf("Starting term...")
	o.log.Printf("Finished term.")

	o.mu.Lock()
	defer o.mu.Unlock()

	// Gather the state of the world from the workers.
	actual := o.collectActual(ctx)
	toAdd, toRemove := o.delta(actual)
	counts := o.counts(actual, toRemove)

	for worker, tasks := range toRemove {
		for _, task := range tasks {
			// Remove the task from the workers.
			o.log.Printf("Removing task %s from %s.", task, worker)
			o.c.Remove(ctx, worker, task)
		}
	}

	for _, task := range toAdd {
		for {
			var tryAgain bool
			counts, tryAgain = o.assignTask(ctx,
				task,
				counts,
				actual,
			)

			if tryAgain {
				// Worker pool was adjusted and task was not assigned.
				continue
			}
			break
		}
	}
}

// assignTask tries to find a worker that does not have too many tasks
// assigned. If it encounters a worker with too many tasks, it will remove
// it from the pool and not assign the task.
func (o *Orchestrator) assignTask(
	ctx context.Context,
	task string,
	counts []countInfo,
	actual map[string][]string,
) (c []countInfo, doOver bool) {

	for i, info := range counts {

		// Ensure that each worker gets an even amount of work assigned.
		// Therefore if a worker gets its fair share, remove it from the worker
		// pool for this term. This also accounts for there being a non-divisbile
		// amount of tasks per workers.
		info.count++
		if info.count > len(o.expectedTasks)/len(actual)+len(o.expectedTasks)%len(actual) {
			counts = append(counts[:i], counts[i+1:]...)

			// Return true saying the worker pool was adjusted and the task was
			// not assigned.
			return counts, true
		}

		// Update the count for the worker.
		counts[i] = countInfo{
			name:  info.name,
			count: info.count,
		}

		// Assign the task to the worker.
		o.log.Printf("Adding task %s to %s.", task, info.name)
		o.c.Add(ctx, info.name, task)
		break
	}
	return counts, false
}

type countInfo struct {
	name  string
	count int
}

// counts looks at each worker and gathers the number of tasks each has.
func (o *Orchestrator) counts(actual, toRemove map[string][]string) []countInfo {
	var results []countInfo
	for k, v := range actual {
		results = append(results, countInfo{
			name:  k,
			count: len(v) - len(toRemove[k]),
		})
	}
	return results
}

// collectActual reaches out to each worker and gets their state of the world.
// Each worker is queried in parallel. If a worker returns an error while
// trying to list the tasks, it will be logged and not considered for what
// workers should be assigned work.
func (o *Orchestrator) collectActual(ctx context.Context) map[string][]string {
	type result struct {
		name   string
		actual []string
		err    error
	}

	results := make(chan result, len(o.workers))
	errs := make(chan result, len(o.workers))
	for _, worker := range o.workers {
		go func(worker string) {
			r, err := o.c.List(ctx, worker)
			if err != nil {
				errs <- result{name: worker, err: err}
				return
			}

			results <- result{name: worker, actual: r}
		}(worker)
	}

	actual := make(map[string][]string)
	for i := 0; i < len(o.workers); i++ {
		select {
		case <-ctx.Done():
			break
		case r := <-results:
			actual[r.name] = r.actual
		case err := <-errs:
			o.log.Printf("Error trying to list tasks from %s: %s", err.name, err.err)
		}
	}

	return actual
}

// delta finds what should be added and removed to make actual match the
// expected.
func (o *Orchestrator) delta(actual map[string][]string) (toAdd []string, toRemove map[string][]string) {
	toRemove = make(map[string][]string)
	expectedTasks := make([]string, len(o.expectedTasks))
	copy(expectedTasks, o.expectedTasks)

	for _, task := range o.expectedTasks {
		if o.hasExpected(task, actual) {
			continue
		}
		toAdd = append(toAdd, task)
	}

	for worker, tasks := range actual {
		for _, task := range tasks {
			if idx := o.contains(task, expectedTasks); idx >= 0 {
				expectedTasks = append(expectedTasks[0:idx], expectedTasks[idx+1:]...)
				continue
			}
			toRemove[worker] = append(toRemove[worker], task)
		}
	}

	return toAdd, toRemove
}

// hasExpected looks at each task in the given actual list and ensures
// a worker node is servicing the task.
func (o *Orchestrator) hasExpected(task string, actual map[string][]string) bool {
	for _, a := range actual {
		if o.contains(task, a) >= 0 {
			return true
		}
	}

	return false
}

// contains returns the index of the given string (x) in the slice y. If the
// string is not present in the slice, it returns -1.
func (o *Orchestrator) contains(x string, y []string) int {
	for i, t := range y {
		if t == x {
			return i
		}
	}

	return -1
}

// AddWorker adds a worker to the known worker cluster. The update will not
// take affect until the next term. It is safe to invoke AddWorker,
// RemoveWorkers and UpdateWorkers on multiple go-routines.
func (o *Orchestrator) AddWorker(worker string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.workers = append(o.workers, worker)
}

// RemoveWorker removes a worker from the known worker cluster. The update
// will not take affect until the next term. It is safe to invoke AddWorker,
// RemoveWorkers and UpdateWorkers on multiple go-routines.
func (o *Orchestrator) RemoveWorker(worker string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	idx := o.contains(worker, o.workers)
	if idx < 0 {
		return
	}

	o.workers = append(o.workers[:idx], o.workers[idx+1:]...)
}

// UpdateWorkers overwrites the expected worker list. The update will not take
// affect until the next term. It is safe to invoke AddWorker, RemoveWorker
// and UpdateWorkers on multiple go-routines.
func (o *Orchestrator) UpdateWorkers(workers []string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.workers = workers
}

// AddTask adds a new task to the expected workload. The update will not take
// affect until the next term. It is safe to invoke AddTask, RemoveTask and
// UpdateTasks on multiple go-routines.
func (o *Orchestrator) AddTask(task string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.expectedTasks = append(o.expectedTasks, task)
}

// RemoveTask removes a task from the expected workload. The update will not
// take affect until the next term. It is safe to invoke AddTask, RemoveTask
// and UpdateTasks on multiple go-routines.
func (o *Orchestrator) RemoveTask(task string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	idx := o.contains(task, o.expectedTasks)
	if idx < 0 {
		return
	}

	o.expectedTasks = append(o.expectedTasks[:idx], o.expectedTasks[idx+1:]...)
}

// UpdateTasks overwrites the expected task list. The update will not take
// affect until the next term. It is safe to invoke AddTask, RemoveTask and
// UpdateTasks on multiple go-routines.
func (o *Orchestrator) UpdateTasks(tasks []string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.expectedTasks = tasks
}
