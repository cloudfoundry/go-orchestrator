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
	"time"
)

// Orchestrator stores the expected workload and reaches out to the cluster
// to see what the actual workload is. It then tries to fix the delta.
//
// The expected task list can be altered via AddTask, RemoveTask and
// UpdateTasks. Each method is safe to be called on multiple go-routines.
type Orchestrator struct {
	log     Logger
	c       Communicator
	timeout time.Duration

	mu            sync.Mutex
	workers       []string
	expectedTasks []Task
}

// New creates a new Orchestrator.
func New(c Communicator, opts ...OrchestratorOption) *Orchestrator {
	o := &Orchestrator{
		c:       c,
		log:     log.New(ioutil.Discard, "", 0),
		timeout: 10 * time.Second,
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}

// Communicator manages the intra communication between the Orchestrator and
// the node cluster. Each method must be safe to call on many go-routines.
// The given context represents the state of the term. Therefore, the
// Communicator is expected to cancel immediately if the context is done.
type Communicator interface {
	// List returns the workload from the given worker.
	List(ctx context.Context, worker string) ([]string, error)

	// Add adds the given task to the worker. The error only logged (for now).
	// It is assumed that if the worker returns an error trying to update, the
	// next term will fix the problem and move the task elsewhere.
	Add(ctx context.Context, worker, task string) error

	// Removes the given task from the worker. The error is only logged (for
	// now). It is assumed that if the worker is returning an error, then it
	// is either not doing the task because the worker is down, or there is a
	// network partition and a future term will fix the problem.
	Remove(ctx context.Context, worker, task string) error
}

//OrchestratorOption configures an Orchestrator.
type OrchestratorOption func(*Orchestrator)

type Logger interface {
	Printf(format string, v ...interface{})
}

// WithLogger sets the logger for the Orchestrator. Defaults to silent logger.
func WithLogger(l Logger) OrchestratorOption {
	return func(o *Orchestrator) {
		o.log = l
	}
}

// WithCommunicatorTimeout sets the timeout for the communication to respond.
// Defaults to 10 seconds.
func WithCommunicatorTimeout(t time.Duration) OrchestratorOption {
	return func(o *Orchestrator) {
		o.timeout = t
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

	// Rebalance tasks among workers.
	toAdd, toRemove = o.rebalance(toAdd, toRemove, actual)
	counts := o.counts(actual, toRemove)

	for worker, tasks := range toRemove {
		for _, task := range tasks {
			// Remove the task from the workers.
			o.log.Printf("Removing task %s from %s.", task, worker)
			removeCtx, _ := context.WithTimeout(ctx, o.timeout)
			o.c.Remove(removeCtx, worker, task)
		}
	}

	for task, missing := range toAdd {
		history := make(map[string]bool)
		for i := 0; i < missing; i++ {
			counts = o.assignTask(ctx,
				task,
				counts,
				actual,
				history,
			)
		}
	}
}

// rebalance will rebalance tasks across the workers. If any worker has too
// many tasks, it will be added to the remove map, and added to the returned
// add slice.
func (o *Orchestrator) rebalance(
	toAdd map[string]int,
	toRemove,
	actual map[string][]string,
) (map[string]int, map[string][]string) {

	counts := o.counts(actual, toRemove)
	var total int
	for _, c := range counts {
		total += c.count
	}

	maxPerNode := total / len(counts)
	if maxPerNode == 0 {
		maxPerNode = 1
	}

	for _, c := range counts {
		if c.count > maxPerNode {
			task := actual[c.name][0]
			o.log.Printf("Worker %s has too many tasks (%d). Moving %s.", c.name, c.count, task)
			toRemove[c.name] = append(toRemove[c.name], task)
			toAdd[task]++
		}
	}

	return toAdd, toRemove
}

// assignTask tries to find a worker that does not have too many tasks
// assigned. If it encounters a worker with too many tasks, it will remove
// it from the pool and try again.
func (o *Orchestrator) assignTask(
	ctx context.Context,
	task string,
	counts []countInfo,
	actual map[string][]string,
	history map[string]bool,
) []countInfo {

	for i, info := range counts {
		// Ensure that each worker gets an even amount of work assigned.
		// Therefore if a worker gets its fair share, remove it from the worker
		// pool for this term. This also accounts for there being a non-divisbile
		// amount of tasks per workers.
		info.count++
		activeWorkers := len(actual)
		if info.count > len(o.expectedTasks)/activeWorkers+len(o.expectedTasks)%activeWorkers {
			counts = append(counts[:i], counts[i+1:]...)

			// Return true saying the worker pool was adjusted and the task was
			// not assigned.
			return o.assignTask(ctx, task, counts, actual, history)
		}

		// Ensure we haven't assigned this task to the worker already.
		if history[info.name] {
			continue
		}
		history[info.name] = true

		// Update the count for the worker.
		counts[i] = countInfo{
			name:  info.name,
			count: info.count,
		}

		// Assign the task to the worker.
		o.log.Printf("Adding task %s to %s.", task, info.name)
		addCtx, _ := context.WithTimeout(ctx, o.timeout)
		o.c.Add(addCtx, info.name, task)
		break
	}

	return counts
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

	listCtx, _ := context.WithTimeout(ctx, o.timeout)
	results := make(chan result, len(o.workers))
	errs := make(chan result, len(o.workers))
	for _, worker := range o.workers {
		go func(worker string) {
			r, err := o.c.List(listCtx, worker)
			if err != nil {
				errs <- result{name: worker, err: err}
				return
			}

			results <- result{name: worker, actual: r}
		}(worker)
	}

	t := time.NewTimer(o.timeout)
	actual := make(map[string][]string)
	for i := 0; i < len(o.workers); i++ {
		select {
		case <-ctx.Done():
			break
		case r := <-results:
			actual[r.name] = r.actual
		case err := <-errs:
			o.log.Printf("Error trying to list tasks from %s: %s", err.name, err.err)
		case <-t.C:
			o.log.Printf("Communicator timeout. Using results available...")
			break
		}
	}

	return actual
}

// delta finds what should be added and removed to make actual match the
// expected.
func (o *Orchestrator) delta(actual map[string][]string) (toAdd map[string]int, toRemove map[string][]string) {
	toRemove = make(map[string][]string)
	toAdd = make(map[string]int)
	expectedTasks := make([]Task, len(o.expectedTasks))
	copy(expectedTasks, o.expectedTasks)

	for _, task := range o.expectedTasks {
		needs := o.hasEnough(task, actual)
		if needs == 0 {
			continue
		}
		toAdd[task.Name] = needs
	}

	for worker, tasks := range actual {
		for _, task := range tasks {
			if idx := o.containsTask(task, expectedTasks); idx >= 0 {
				expectedTasks = append(expectedTasks[0:idx], expectedTasks[idx+1:]...)
				continue
			}
			toRemove[worker] = append(toRemove[worker], task)
		}
	}

	return toAdd, toRemove
}

// hasEnough looks at each task in the given actual list and ensures
// a worker node is servicing the task.
func (o *Orchestrator) hasEnough(t Task, actual map[string][]string) (needs int) {
	var count int
	for _, a := range actual {
		if o.contains(t.Name, a) >= 0 {
			count++
		}
	}

	return t.Instances - count
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

// containsTask returns the index of the given task name in the tasks. If the
// task is not found, it returns -1.
func (o *Orchestrator) containsTask(task string, tasks []Task) int {
	for i, t := range tasks {
		if t.Name == task {
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

// Task stores the required information for a task.
type Task struct {
	Name      string
	Instances int
}

// AddTask adds a new task to the expected workload. The update will not take
// affect until the next term. It is safe to invoke AddTask, RemoveTask and
// UpdateTasks on multiple go-routines.
func (o *Orchestrator) AddTask(task string, opts ...TaskOption) {
	o.mu.Lock()
	defer o.mu.Unlock()

	t := Task{Name: task, Instances: 1}
	for _, opt := range opts {
		opt(&t)
	}

	o.expectedTasks = append(o.expectedTasks, t)
}

// TaskOption is used to configure a task when it is being added.
type TaskOption func(*Task)

// WithTaskInstances configures the number of tasks. Defaults to 1.
func WithTaskInstances(i int) TaskOption {
	return func(t *Task) {
		t.Instances = i
	}
}

// RemoveTask removes a task from the expected workload. The update will not
// take affect until the next term. It is safe to invoke AddTask, RemoveTask
// and UpdateTasks on multiple go-routines.
func (o *Orchestrator) RemoveTask(task string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	idx := o.containsTask(task, o.expectedTasks)
	if idx < 0 {
		return
	}

	o.expectedTasks = append(o.expectedTasks[:idx], o.expectedTasks[idx+1:]...)
}

// UpdateTasks overwrites the expected task list. The update will not take
// affect until the next term. It is safe to invoke AddTask, RemoveTask and
// UpdateTasks on multiple go-routines.
func (o *Orchestrator) UpdateTasks(tasks []Task) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.expectedTasks = tasks
}
