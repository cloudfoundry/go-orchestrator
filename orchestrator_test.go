package orchestrator_test

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	orchestrator "code.cloudfoundry.org/go-orchestrator"
	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
)

type TO struct {
	*testing.T
	spy          *spyCommunicator
	o            *orchestrator.Orchestrator
	statsHandler func(orchestrator.TermStats)
}

func TestOrchestrator(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TO {
		spy := newSpyCommunicator()
		logger := log.New(ioutil.Discard, "", 0)
		if testing.Verbose() {
			logger = log.New(os.Stderr, "[Orchestrator] ", 0)
		}

		return TO{
			T: t,
			o: orchestrator.New(
				spy,
				orchestrator.WithLogger(logger),
				orchestrator.WithCommunicatorTimeout(time.Millisecond),
			),
			spy: spy,
		}
	})

	o.Group("with 3 worker nodes and 3 tasks", func() {
		o.BeforeEach(func(t TO) TO {
			for i := 0; i < 3; i++ {
				t.o.AddWorker(fmt.Sprintf("worker-%d", i))
				t.o.AddTask(fmt.Sprintf("task-%d", i))
			}

			// Can tolerate the same worker and task added twice
			t.o.AddWorker("worker-0")
			t.o.AddTask("task-0")
			return t
		})

		o.Group("with no tasks yet assigned", func() {
			o.Spec("it evenly splits tasks among the cluster", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spy.added).To(HaveLen(3))

				Expect(t, t.spy.added["worker-0"]).To(HaveLen(1))
				Expect(t, t.spy.added["worker-1"]).To(HaveLen(1))
				Expect(t, t.spy.added["worker-2"]).To(HaveLen(1))

				Expect(t, t.o.ListExpectedTasks()).To(HaveLen(3))
				Expect(t, t.o.LastActual()).To(HaveLen(3))

				Expect(t, append(append(
					t.spy.added["worker-0"],
					t.spy.added["worker-1"]...),
					t.spy.added["worker-2"]...,
				)).To(Contain(
					"task-0",
					"task-1",
					"task-2",
				))

				ctx := t.spy.listCtx["worker-0"][0]
				_, ok := ctx.Deadline()
				Expect(t, ok).To(BeTrue())

				ctx = t.spy.addedCtx["worker-0"][0]
				_, ok = ctx.Deadline()
				Expect(t, ok).To(BeTrue())
			})

		})

		o.Group("stats", func() {
			o.Spec("it reports that 2 workers responded to List", func(t TO) {
				var stats orchestrator.TermStats
				t.o = orchestrator.New(
					t.spy,
					orchestrator.WithStats(func(s orchestrator.TermStats) {
						stats = s
					}),
				)
				t.o.AddWorker("worker-0")
				t.o.AddWorker("worker-1")
				t.o.NextTerm(context.Background())

				Expect(t, stats.WorkerCount).To(Equal(2))
			})
		})

		o.Group("with task that has 2 instances", func() {
			o.BeforeEach(func(t TO) TO {
				t.o.UpdateTasks(nil)
				return t
			})

			o.Spec("it assigns the task to 2 different workers", func(t TO) {
				t.o.AddTask("multi-task", orchestrator.WithTaskInstances(3))
				t.o.NextTerm(context.Background())

				Expect(t, count("multi-task", append(append(
					t.spy.added["worker-0"],
					t.spy.added["worker-1"]...),
					t.spy.added["worker-2"]...,
				))).To(Equal(3))

				Expect(t, count("multi-task", t.spy.added["worker-0"])).To(BeBelow(1))
				Expect(t, count("multi-task", t.spy.added["worker-1"])).To(BeBelow(1))
				Expect(t, count("multi-task", t.spy.added["worker-2"])).To(BeBelow(1))
			})

			o.Spec("it removes stale task", func(t TO) {
				t.spy.actual["worker-0"] = []interface{}{"multi-task-0", "multi-task-1"}
				t.spy.actual["worker-1"] = []interface{}{"multi-task-0", "multi-task-1"}
				t.o.AddTask("multi-task-0", orchestrator.WithTaskInstances(2))
				t.o.NextTerm(context.Background())

				Expect(t, t.spy.removed["worker-0"]).To(HaveLen(1))
				Expect(t, t.spy.removed["worker-1"]).To(HaveLen(1))
			})

			o.Spec("it removes any extra tasks", func(t TO) {
				t.spy.actual["worker-0"] = []interface{}{"multi-task"}
				t.spy.actual["worker-1"] = []interface{}{"multi-task"}
				t.spy.actual["worker-2"] = []interface{}{"multi-task"}
				t.o.AddTask("multi-task", orchestrator.WithTaskInstances(2))
				t.o.NextTerm(context.Background())

				Expect(t, t.spy.removed).To(HaveLen(1))
			})

			o.Group("with single worker", func() {
				o.Spec("it only assigns the task once", func(t TO) {
					t.o.UpdateWorkers([]interface{}{"worker"})
					t.spy.actual["worker"] = []interface{}{"multi-task"}
					t.o.AddTask("multi-task", orchestrator.WithTaskInstances(2))

					t.o.NextTerm(context.Background())

					Expect(t, t.spy.added["worker"]).To(HaveLen(0))
				})
			})

			o.Group("with more total tasks than workers", func() {
				o.Spec("it assigns each task to 2 different workers", func(t TO) {
					t.o.AddTask("multi-task-0", orchestrator.WithTaskInstances(2))
					t.o.AddTask("multi-task-1", orchestrator.WithTaskInstances(2))
					t.o.AddTask("multi-task-2", orchestrator.WithTaskInstances(2))
					t.o.NextTerm(context.Background())

					Expect(t, count("multi-task-0", append(append(
						t.spy.added["worker-0"],
						t.spy.added["worker-1"]...),
						t.spy.added["worker-2"]...,
					))).To(Equal(2))

					Expect(t, count("multi-task-1", append(append(
						t.spy.added["worker-0"],
						t.spy.added["worker-1"]...),
						t.spy.added["worker-2"]...,
					))).To(Equal(2))

					Expect(t, count("multi-task-2", append(append(
						t.spy.added["worker-0"],
						t.spy.added["worker-1"]...),
						t.spy.added["worker-2"]...,
					))).To(Equal(2))
				})
			})
		})

		o.Group("with one task assigned", func() {
			o.BeforeEach(func(t TO) TO {
				t.spy.actual["worker-0"] = []interface{}{"task-1"}
				return t
			})

			o.Spec("it does not replace the existing task", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spy.added).To(HaveLen(2))

				Expect(t, t.spy.added["worker-0"]).To(HaveLen(0))
				Expect(t, t.spy.added["worker-1"]).To(HaveLen(1))
				Expect(t, t.spy.added["worker-2"]).To(HaveLen(1))

				var all []interface{}
				for _, tasks := range t.spy.added {
					all = append(all, tasks...)
				}

				Expect(t, all).To(Contain("task-0", "task-2"))
			})
		})

		o.Group("with extra task", func() {
			o.BeforeEach(func(t TO) TO {
				t.spy.actual["worker-0"] = []interface{}{"extra"}
				return t
			})

			o.Spec("it removes the task", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spy.added).To(HaveLen(3))
				Expect(t, t.spy.removed).To(HaveLen(1))

				Expect(t, t.spy.removed["worker-0"]).To(HaveLen(1))
				Expect(t, t.spy.removed["worker-1"]).To(HaveLen(0))
				Expect(t, t.spy.removed["worker-2"]).To(HaveLen(0))

				Expect(t, t.spy.removed["worker-0"]).To(Contain("extra"))

				ctx := t.spy.removedCtx["worker-0"][0]
				_, ok := ctx.Deadline()
				Expect(t, ok).To(BeTrue())
			})
		})

		o.Group("with too many of a task", func() {
			o.BeforeEach(func(t TO) TO {
				t.spy.actual["worker-0"] = []interface{}{"task-0"}
				t.spy.actual["worker-1"] = []interface{}{"task-0"}
				return t
			})

			o.Spec("it removes the task and adds the required", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spy.removed).To(HaveLen(1))
				Expect(t, t.spy.added).To(HaveLen(2))
			})
		})

		o.Group("with a worker removed", func() {
			o.BeforeEach(func(t TO) TO {
				t.spy.actual["worker-0"] = []interface{}{"task-0"}
				t.spy.actual["worker-2"] = []interface{}{"task-2"}

				t.o.RemoveWorker("worker-1")
				return t
			})

			o.Spec("divvys up work amongst the remaining workers", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spy.added["worker-1"]).To(HaveLen(0))
				Expect(t, append(
					t.spy.added["worker-0"],
					t.spy.added["worker-2"]...,
				)).To(HaveLen(1))

				Expect(t, append(
					t.spy.added["worker-0"],
					t.spy.added["worker-2"]...,
				)).To(Contain(
					"task-1",
				))
			})
		})

		o.Group("with a worker who is empty", func() {
			o.BeforeEach(func(t TO) TO {
				t.spy.actual["worker-0"] = []interface{}{"task-0", "task-1"}
				t.spy.actual["worker-2"] = []interface{}{"task-2"}
				return t
			})

			o.Spec("evens out the tasks amongst the workers", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spy.added["worker-1"]).To(HaveLen(1))
				Expect(t, t.spy.removed["worker-0"]).To(HaveLen(1))
			})
		})

		o.Group("with number of tasks doesn't cleanly fit in", func() {
			o.BeforeEach(func(t TO) TO {
				t.o.AddTask("task-3")
				t.spy.actual["worker-0"] = []interface{}{"task-0", "task-3"}
				t.spy.actual["worker-1"] = []interface{}{"task-1"}
				t.spy.actual["worker-2"] = []interface{}{"task-2"}
				return t
			})

			o.Spec("doesn't move anything around", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spy.removed["worker-0"]).To(HaveLen(0))
			})
		})

		o.Group("with a workers updated", func() {
			o.BeforeEach(func(t TO) TO {
				t.spy.actual["worker-0"] = []interface{}{"task-0"}
				t.spy.actual["worker-2"] = []interface{}{"task-2"}

				t.o.UpdateWorkers([]interface{}{"worker-0", "worker-2"})
				return t
			})

			o.Spec("divvys up work amongst the remaining workers", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spy.added["worker-1"]).To(HaveLen(0))
				Expect(t, append(
					t.spy.added["worker-0"],
					t.spy.added["worker-2"]...,
				)).To(HaveLen(1))

				Expect(t, append(
					t.spy.added["worker-0"],
					t.spy.added["worker-2"]...,
				)).To(Contain(
					"task-1",
				))
			})
		})

		o.Group("with a task removed", func() {
			o.BeforeEach(func(t TO) TO {
				t.spy.actual["worker-0"] = []interface{}{"task-0"}
				t.spy.actual["worker-1"] = []interface{}{"task-1"}
				t.spy.actual["worker-2"] = []interface{}{"task-2"}

				t.o.RemoveTask("task-1")
				return t
			})

			o.Spec("removes the tasks from the worker", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spy.removed["worker-1"]).To(HaveLen(1))
				Expect(t, t.spy.removed["worker-1"]).To(Contain("task-1"))

				Expect(t, append(append(
					t.spy.added["worker-0"],
					t.spy.added["worker-1"]...),
					t.spy.added["worker-2"]...,
				)).To(HaveLen(0))
			})
		})

		o.Group("with task list updated", func() {
			o.BeforeEach(func(t TO) TO {
				t.spy.actual["worker-0"] = []interface{}{"task-0"}
				t.spy.actual["worker-1"] = []interface{}{"task-1"}
				t.spy.actual["worker-2"] = []interface{}{"task-2"}

				t.o.UpdateTasks([]orchestrator.Task{
					{
						Name:      "task-0",
						Instances: 1,
					},
					{
						Name:      "task-2",
						Instances: 1,
					},
				})
				return t
			})

			o.Spec("removes the tasks from the worker", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spy.removed["worker-1"]).To(HaveLen(1))
				Expect(t, t.spy.removed["worker-1"]).To(Contain("task-1"))

				Expect(t, append(append(
					t.spy.added["worker-0"],
					t.spy.added["worker-1"]...),
					t.spy.added["worker-2"]...,
				)).To(HaveLen(0))
			})
		})

		o.Group("with a worker returning an error for list", func() {
			o.BeforeEach(func(t TO) TO {
				t.spy.listErrs["worker-1"] = errors.New("some-error")

				return t
			})

			o.Spec("divvys up work amongst the remaining workers", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spy.added["worker-1"]).To(HaveLen(0))
				Expect(t, append(
					t.spy.added["worker-0"],
					t.spy.added["worker-2"]...,
				)).To(HaveLen(3))

				Expect(t, append(
					t.spy.added["worker-0"],
					t.spy.added["worker-2"]...,
				)).To(Contain(
					"task-0",
					"task-1",
					"task-2",
				))
			})
		})

		o.Group("with a worker blocking trying to list", func() {
			o.BeforeEach(func(t TO) TO {
				t.spy.block["worker-1"] = true

				return t
			})

			o.Spec("divvys up work amongst the remaining workers", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spy.added["worker-1"]).To(HaveLen(0))
				Expect(t, append(
					t.spy.added["worker-0"],
					t.spy.added["worker-2"]...,
				)).To(HaveLen(3))

				Expect(t, append(
					t.spy.added["worker-0"],
					t.spy.added["worker-2"]...,
				)).To(Contain(
					"task-0",
					"task-1",
					"task-2",
				))
			})
		})
	})

	o.Spec("handles having 0 workers", func(t TO) {
		t.o.NextTerm(context.Background())
	})
}

type spyCommunicator struct {
	mu sync.Mutex

	block      map[interface{}]bool
	listErrs   map[interface{}]error
	actual     map[interface{}][]interface{}
	added      map[interface{}][]interface{}
	removed    map[interface{}][]interface{}
	listCtx    map[interface{}][]context.Context
	addedCtx   map[interface{}][]context.Context
	removedCtx map[interface{}][]context.Context
}

func newSpyCommunicator() *spyCommunicator {
	return &spyCommunicator{
		block:      make(map[interface{}]bool),
		listCtx:    make(map[interface{}][]context.Context),
		listErrs:   make(map[interface{}]error),
		actual:     make(map[interface{}][]interface{}),
		added:      make(map[interface{}][]interface{}),
		addedCtx:   make(map[interface{}][]context.Context),
		removedCtx: make(map[interface{}][]context.Context),
		removed:    make(map[interface{}][]interface{}),
	}
}

func (s *spyCommunicator) List(ctx context.Context, worker interface{}) ([]interface{}, error) {
	if s.block[worker] {
		var c chan int
		<-c
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.listCtx[worker] = append(s.listCtx[worker], ctx)
	return s.actual[worker], s.listErrs[worker]
}

func (s *spyCommunicator) Add(ctx context.Context, worker interface{}, task interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.added[worker] = append(s.added[worker], task)
	s.addedCtx[worker] = append(s.addedCtx[worker], ctx)
	return nil
}

func (s *spyCommunicator) Remove(ctx context.Context, worker interface{}, task interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.removed[worker] = append(s.removed[worker], task)
	s.removedCtx[worker] = append(s.removedCtx[worker], ctx)
	return nil
}

func count(x interface{}, y []interface{}) int {
	var total int
	for _, s := range y {
		if s == x {
			total++
		}
	}
	return total
}
