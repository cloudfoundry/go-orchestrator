// Package orchestrator is an algorithm that manages the work of a cluster of
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
package orchestrator

import (
	"context"
	"io/ioutil"
	"log"
	"sync"
	"time"
)

// Communicator manages the internal communication between the Orchestrator and
// the node cluster. Each method must be safe to call on many go-routines.
// The given context represents the state of the term. Therefore, the
// Communicator is expected to cancel immediately if the context is done.
type Communicator interface {
	// List returns the workload from the given worker.
	List(ctx context.Context) ([]interface{}, error)

	// Add adds the given task to the worker. The error only logged (for now).
	// It is assumed that if the worker returns an error trying to update, the
	// next term will fix the problem and move the task elsewhere.
	Add(ctx context.Context, taskDefinition interface{}) error

	// Removes the given task from the worker. The error is only logged (for
	// now). It is assumed that if the worker is returning an error, then it
	// is either not doing the task because the worker is down, or there is a
	// network partition and a future term will fix the problem.
	Remove(ctx context.Context, taskDefinition interface{}) error
}

type Worker struct {
	Identifier interface{}
	Communicator
}

// Orchestrator stores the expected workload and reaches out to the cluster
// to see what the actual workload is. It then tries to fix the delta.
//
// The expected task list can be altered via AddTask, RemoveTask and
// UpdateTasks. Each method is safe to be called on multiple go-routines.
type Orchestrator struct {
	log     Logger
	s       func(TermStats)
	timeout time.Duration

	mu            sync.Mutex
	workers       []Worker
	expectedTasks []Task

	// LastActual is set each term. It is only used for a user who wants to
	// know the state of the worker cluster from the last term.
	lastActual []WorkerState
}

// New creates a new Orchestrator.
func New(opts ...OrchestratorOption) *Orchestrator {
	o := &Orchestrator{
		s:       func(TermStats) {},
		log:     log.New(ioutil.Discard, "", 0),
		timeout: 10 * time.Second,
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}

// NextTerm reaches out to the cluster to gather to actual workload. It then
// attempts to fix the delta between actual and expected. The lifecycle of
// the term is managed by the given context.
func (o *Orchestrator) NextTerm(ctx context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Gather the state of the world from the workers.
	actual := o.collectActual(ctx)
	toAdd, toRemove := o.delta(actual)

	// Rebalance tasks among workers.
	toAdd, toRemove = rebalance(toAdd, toRemove, actual)
	counts := counts(actual, toRemove)

	for worker, tasks := range toRemove {
		for _, task := range tasks {
			// Remove the task from the workers.
			removeCtx, _ := context.WithTimeout(ctx, o.timeout)
			worker.Remove(removeCtx, task)
		}
	}

	for taskDefinition, missing := range toAdd {
		history := make(map[Worker]bool)
		for i := 0; i < missing; i++ {
			counts = o.assignTask(ctx,
				taskDefinition,
				counts,
				actual,
				history,
			)
		}
	}

	o.s(TermStats{
		WorkerCount: len(actual),
	})
}

// collectActual reaches out to each worker and gets their state of the world.
// Each worker is queried in parallel. If a worker returns an error while
// trying to list the tasks, it will be logged and not considered for what
// workers should be assigned work.
func (o *Orchestrator) collectActual(ctx context.Context) map[Worker][]interface{} {
	type result struct {
		worker Worker
		actual []interface{}
		err    error
	}

	listCtx, _ := context.WithTimeout(ctx, o.timeout)
	results := make(chan result, len(o.workers))
	errs := make(chan result, len(o.workers))
	for _, worker := range o.workers {
		go func(worker Worker) {
			listResults, err := worker.List(listCtx)
			if err != nil {
				errs <- result{worker: worker, err: err}
				return
			}

			results <- result{worker: worker, actual: listResults}
		}(worker)
	}

	t := time.NewTimer(o.timeout)
	var state []WorkerState
	actual := make(map[Worker][]interface{})
	for i := 0; i < len(o.workers); i++ {
		select {
		case <-ctx.Done():
			break
		case nextResult := <-results:
			actual[nextResult.worker] = nextResult.actual
			state = append(state, WorkerState{Worker: nextResult.worker, Tasks: nextResult.actual})
		case err := <-errs:
			o.log.Printf("Error trying to list tasks from %s: %s", err.worker, err.err)
		case <-t.C:
			o.log.Printf("Communicator timeout. Using results available...")
			break
		}
	}

	o.lastActual = state
	return actual
}

// delta finds what should be added and removed to make actual match the
// expected.
func (o *Orchestrator) delta(actual map[Worker][]interface{}) (toAdd map[interface{}]int, toRemove map[Worker][]interface{}) {
	toAdd = make(map[interface{}]int)
	toRemove = make(map[Worker][]interface{})

	expectedTasks := make([]Task, len(o.expectedTasks))
	copy(expectedTasks, o.expectedTasks)

	for _, task := range o.expectedTasks {
		needs := hasEnoughInstances(task, actual)
		if needs == 0 {
			continue
		}
		toAdd[task.Definition] = needs
	}

	for worker, tasks := range actual {
		for _, task := range tasks {
			if idx := containsTask(task, expectedTasks); idx >= 0 {
				expectedTasks[idx].Instances--
				if expectedTasks[idx].Instances == 0 {
					expectedTasks = append(expectedTasks[0:idx], expectedTasks[idx+1:]...)
				}
				continue
			}
			toRemove[worker] = append(toRemove[worker], task)
		}
	}

	return toAdd, toRemove
}

// assignTask tries to find a worker that does not have too many tasks
// assigned. If it encounters a worker with too many tasks, it will remove
// it from the pool and try again.
func (o *Orchestrator) assignTask(
	ctx context.Context,
	taskDefinition interface{},
	workerLoads []workerLoad,
	actual map[Worker][]interface{},
	history map[Worker]bool,
) []workerLoad {
	activeWorkers := len(actual)
	if activeWorkers == 0 {
		return workerLoads
	}

	totalTasks := o.totalTaskCount()
	maxTaskCount := totalTasks/activeWorkers + totalTasks%activeWorkers

	for i, loadInfo := range workerLoads {
		// Ensure that each worker gets an even amount of work assigned.
		// Therefore if a worker gets its fair share, remove it from the worker
		// pool for this term. This also accounts for there being a non-divisible
		// amount of tasks per workers.
		loadInfo.taskCount++
		if loadInfo.taskCount > maxTaskCount {
			workerLoads = append(workerLoads[:i], workerLoads[i+1:]...)

			// Recurse since the worker pool was adjusted and the task was
			// not assigned.
			return o.assignTask(ctx, taskDefinition, workerLoads, actual, history)
		}

		// Ensure we haven't assigned this task to the worker already.
		if history[loadInfo.worker] || contains(taskDefinition, actual[loadInfo.worker]) >= 0 {
			continue
		}
		history[loadInfo.worker] = true

		// Assign the task to the worker.
		o.log.Printf("Adding task %s to %s.", taskDefinition, loadInfo.worker)
		addCtx, _ := context.WithTimeout(ctx, o.timeout)
		loadInfo.worker.Add(addCtx, taskDefinition)

		// Move updated count to end of slice to help with fairness
		workerLoads = append(
			append(workerLoads[:i], workerLoads[i+1:]...),
			workerLoad{
				worker:    loadInfo.worker,
				taskCount: loadInfo.taskCount,
			},
		)

		break
	}

	return workerLoads
}

// totalTaskCount calculates the total number of expected task instances.
func (o *Orchestrator) totalTaskCount() int {
	var total int
	for _, t := range o.expectedTasks {
		total += t.Instances
	}
	return total
}

// AddWorker adds a worker to the known worker cluster. The update will not
// take affect until the next term. It is safe to invoke AddWorker,
// RemoveWorkers and UpdateWorkers on multiple go-routines.
func (o *Orchestrator) AddWorker(worker Worker) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Ensure we don't already have this worker
	idx := containsWorker(worker, o.workers)
	if idx > -1 {
		return
	}

	o.workers = append(o.workers, worker)
}

// RemoveWorker removes a worker from the known worker cluster. The update
// will not take affect until the next term. It is safe to invoke AddWorker,
// RemoveWorkers and UpdateWorkers on multiple go-routines.
func (o *Orchestrator) RemoveWorker(worker Worker) {
	o.mu.Lock()
	defer o.mu.Unlock()

	idx := containsWorker(worker, o.workers)
	if idx < 0 {
		return
	}

	o.workers = append(o.workers[:idx], o.workers[idx+1:]...)
}

// UpdateWorkers overwrites the expected worker list. The update will not take
// affect until the next term. It is safe to invoke AddWorker, RemoveWorker
// and UpdateWorkers on multiple go-routines.
func (o *Orchestrator) UpdateWorkers(workers []Worker) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.workers = workers
}

// Task stores the required information for a task.
type Task struct {
	Definition interface{}
	Instances  int
}

// AddTask adds a new task to the expected workload. The update will not take
// affect until the next term. It is safe to invoke AddTask, RemoveTask and
// UpdateTasks on multiple go-routines.
func (o *Orchestrator) AddTask(taskDefinition interface{}, opts ...TaskOption) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Ensure we don't already have this task
	for _, t := range o.expectedTasks {
		if taskDefinition == t.Definition {
			return
		}
	}

	t := Task{Definition: taskDefinition, Instances: 1}
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
func (o *Orchestrator) RemoveTask(taskDefinition interface{}) {
	o.mu.Lock()
	defer o.mu.Unlock()

	idx := containsTask(taskDefinition, o.expectedTasks)
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

// ListExpectedTasks returns the current list of the expected tasks.
func (o *Orchestrator) ListExpectedTasks() []Task {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.expectedTasks
}

// WorkerState stores the state of a worker.
type WorkerState struct {
	Worker Worker

	// Tasks are the task definitions the worker is servicing.
	Tasks []interface{}
}

// LastActual returns the actual from the last term. It will return nil
// before the first term.
func (o *Orchestrator) LastActual() []WorkerState {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.lastActual
}

// rebalance will rebalance tasks across the workers. If any worker has too
// many tasks, it will be added to the remove map, and added to the returned
// add slice.
func rebalance(
	toAdd map[interface{}]int,
	toRemove,
	actual map[Worker][]interface{},
) (map[interface{}]int, map[Worker][]interface{}) {

	counts := counts(actual, toRemove)
	if len(counts) == 0 {
		return toAdd, toRemove
	}

	var total int
	for _, c := range counts {
		total += c.taskCount
	}

	for _, addCount := range toAdd {
		total += addCount
	}

	maxPerNode := total / len(counts)
	if maxPerNode == 0 || total%len(counts) != 0 {
		maxPerNode++
	}

	for _, c := range counts {
		if c.taskCount > maxPerNode {
			task := actual[c.worker][0]
			toRemove[c.worker] = append(toRemove[c.worker], task)
			toAdd[task]++
		}
	}

	return toAdd, toRemove
}

// hasEnoughInstances looks at each task in the given actual list and ensures
// a worker node is servicing the task.
func hasEnoughInstances(t Task, actual map[Worker][]interface{}) (needs int) {
	var count int
	for _, a := range actual {
		if contains(t.Definition, a) >= 0 {
			count++
		}
	}

	return t.Instances - count
}

// contains returns the index of the given interface{} (x) in the slice y. If the
// interface{} is not present in the slice, it returns -1.
func contains(x interface{}, y []interface{}) int {
	for i, t := range y {
		if t == x {
			return i
		}
	}

	return -1
}

// containsTask returns the index of the given task name in the tasks. If the
// task is not found, it returns -1.
func containsTask(task interface{}, tasks []Task) int {
	for i, t := range tasks {
		if t.Definition == task {
			return i
		}
	}

	return -1
}

// containsWorker returns the index of the given worker name in the workers. If the
// worker is not found, it returns -1.
func containsWorker(worker Worker, workers []Worker) int {
	for i, w := range workers {
		if w.Identifier == worker.Identifier {
			return i
		}
	}

	return -1
}
