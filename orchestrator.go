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
	"log"
	"sync"
)

// Orchestrator stores the expected workload and reaches out to the cluster
// to see what the actual workload is. It then tries to fix the delta.
//
// The expected task list can be altered via AddTask, RemoveTask and
// UpdateTasks. Each method is safe to be called on multiple go-routines.
type Orchestrator struct {
	log *log.Logger
	c   Communicator

	mu            sync.Mutex
	workers       []string
	expectedTasks []string
}

// New creates a new Orchestrator.
func New(c Communicator, opts ...OrchestratorOption) *Orchestrator {
	o := &Orchestrator{
		c: c,
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}

// Communicator manages the intra communication between the Orchestrator and
// the node cluster.
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

// WithLogger sets the logger for the Orchestrator.
func WithLogger(l *log.Logger) OrchestratorOption {
	return func(o *Orchestrator) {
		o.log = l
	}
}

// NextTerm reaches out to the cluster to gather to actual workload. It then
// attempts to fix the delta between actual and expected. The lifecycle of
// the term is managed by the given context.
func (o *Orchestrator) NextTerm(ctx context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()

	actual := o.collectActual(ctx)
	toAdd, toRemove := o.delta(actual)
	counts := o.counts(actual, toRemove)

	for worker, tasks := range toRemove {
		for _, task := range tasks {
			o.c.Remove(ctx, worker, task)
		}
	}

	for _, task := range toAdd {
		for worker, count := range counts {
			count++
			if count > len(o.expectedTasks)/len(actual) {
				delete(counts, worker)
				continue
			}
			counts[worker] = count

			o.c.Add(ctx, worker, task)
			break
		}
	}
}

func (o *Orchestrator) counts(actual, toRemove map[string][]string) map[string]int {
	m := make(map[string]int)
	for k, v := range actual {
		m[k] = len(v) - len(toRemove[k])
	}
	return m
}

func (o *Orchestrator) collectActual(ctx context.Context) map[string][]string {
	type result struct {
		name   string
		actual []string
	}

	results := make(chan result, len(o.workers))
	for _, worker := range o.workers {
		go func(worker string) {
			// TODO: handle errs
			r, _ := o.c.List(ctx, worker)

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
		}
	}

	return actual
}

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

func (o *Orchestrator) hasExpected(task string, actual map[string][]string) bool {
	for _, a := range actual {
		if o.contains(task, a) >= 0 {
			return true
		}
	}

	return false
}

func (o *Orchestrator) contains(task string, tasks []string) int {
	for i, t := range tasks {
		if t == task {
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
}

// UpdateWorkers overwrites the expected worker list. The update will not take
// affect until the next term. It is safe to invoke AddWorker, RemoveWorker
// and UpdateWorkers on multiple go-routines.
func (o *Orchestrator) UpdateWorkers(workers []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
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
}

// UpdateTasks overwrites the expected task list. The update will not take
// affect until the next term. It is safe to invoke AddTask, RemoveTask and
// UpdateTasks on multiple go-routines.
func (o *Orchestrator) UpdateTasks(tasks []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
}
