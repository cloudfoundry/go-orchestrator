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

	"code.cloudfoundry.org/go-orchestrator"
	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
)

type TO struct {
	*testing.T
	spyCommunicators map[string]*spyCommunicator
	o                *orchestrator.Orchestrator
	statsHandler     func(orchestrator.TermStats)
}

func TestOrchestrator(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TO {
		logger := log.New(ioutil.Discard, "", 0)
		if testing.Verbose() {
			logger = log.New(os.Stderr, "[Orchestrator] ", 0)
		}

		return TO{
			T: t,
			o: orchestrator.New(
				orchestrator.WithLogger(logger),
				orchestrator.WithCommunicatorTimeout(10*time.Millisecond),
			),
			spyCommunicators: make(map[string]*spyCommunicator),
		}
	})

	o.Group("with 3 worker nodes and 3 tasks", func() {
		o.BeforeEach(func(t TO) TO {
			for i := 0; i < 3; i++ {
				workerId := fmt.Sprintf("worker-%d", i)
				communicator := newSpyCommunicator()
				t.spyCommunicators[workerId] = communicator

				t.o.AddWorker(orchestrator.Worker{Identifier: workerId, Communicator: communicator})
				t.o.AddTask(fmt.Sprintf("task-%d", i))
			}

			// Can tolerate the same worker and task added twice
			t.o.AddWorker(orchestrator.Worker{Identifier: "worker-0"})
			t.o.AddTask("task-0")
			return t
		})

		o.Group("with no tasks yet assigned", func() {
			o.Spec("it evenly splits tasks among the cluster", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spyCommunicators["worker-0"].added).To(HaveLen(1))
				Expect(t, t.spyCommunicators["worker-1"].added).To(HaveLen(1))
				Expect(t, t.spyCommunicators["worker-2"].added).To(HaveLen(1))

				Expect(t, t.o.ListExpectedTasks()).To(HaveLen(3))
				Expect(t, t.o.LastActual()).To(HaveLen(3))

				Expect(t, append(append(
					t.spyCommunicators["worker-0"].added,
					t.spyCommunicators["worker-1"].added...),
					t.spyCommunicators["worker-2"].added...,
				)).To(Contain(
					"task-0",
					"task-1",
					"task-2",
				))

				ctx := t.spyCommunicators["worker-0"].listCtx[0]
				_, ok := ctx.Deadline()
				Expect(t, ok).To(BeTrue())

				ctx = t.spyCommunicators["worker-0"].addedCtx[0]
				_, ok = ctx.Deadline()
				Expect(t, ok).To(BeTrue())
			})
		})

		o.Group("stats", func() {
			o.Spec("it reports that 2 workers responded to List", func(t TO) {
				var stats orchestrator.TermStats
				orch := orchestrator.New(
					orchestrator.WithStats(func(s orchestrator.TermStats) {
						stats = s
					}),
				)
				orch.AddWorker(orchestrator.Worker{Identifier: "worker-0", Communicator: newSpyCommunicator()})
				orch.AddWorker(orchestrator.Worker{Identifier: "worker-1", Communicator: newSpyCommunicator()})
				orch.NextTerm(context.Background())

				Expect(t, stats.WorkerCount).To(Equal(2))
			})
		})

		o.Group("with task that has multiple instances", func() {
			o.BeforeEach(func(t TO) TO {
				t.o.UpdateTasks(nil)
				return t
			})

			o.Spec("it assigns the task to 3 different workers", func(t TO) {
				t.o.AddTask("multi-task", orchestrator.WithTaskInstances(3))
				t.o.NextTerm(context.Background())

				Expect(t, count("multi-task", append(append(
					t.spyCommunicators["worker-0"].added,
					t.spyCommunicators["worker-1"].added...),
					t.spyCommunicators["worker-2"].added...,
				))).To(Equal(3))

				Expect(t, count("multi-task", t.spyCommunicators["worker-0"].added)).To(BeBelow(2))
				Expect(t, count("multi-task", t.spyCommunicators["worker-1"].added)).To(BeBelow(2))
				Expect(t, count("multi-task", t.spyCommunicators["worker-2"].added)).To(BeBelow(2))
			})

			o.Spec("rebalances without touching existing workers", func(t TO) {
				for i := 0; i < 7; i++ {
					t.spyCommunicators["worker-0"].actual = append(t.spyCommunicators["worker-0"].actual, i)
					t.o.AddTask(i, orchestrator.WithTaskInstances(3))
				}
				t.o.NextTerm(context.Background())

				Expect(t, t.spyCommunicators["worker-0"].removed).To(HaveLen(0))
			})

			o.Spec("it removes stale task", func(t TO) {
				t.spyCommunicators["worker-0"].actual = []interface{}{"multi-task-0", "multi-task-1"}
				t.spyCommunicators["worker-1"].actual = []interface{}{"multi-task-0", "multi-task-1"}
				t.o.AddTask("multi-task-0", orchestrator.WithTaskInstances(2))
				t.o.NextTerm(context.Background())

				Expect(t, t.spyCommunicators["worker-0"].removed).To(HaveLen(1))
				Expect(t, t.spyCommunicators["worker-1"].removed).To(HaveLen(1))
			})

			o.Spec("it removes any extra tasks", func(t TO) {
				t.spyCommunicators["worker-0"].actual = []interface{}{"multi-task"}
				t.spyCommunicators["worker-1"].actual = []interface{}{"multi-task"}
				t.spyCommunicators["worker-2"].actual = []interface{}{"multi-task"}
				t.o.AddTask("multi-task", orchestrator.WithTaskInstances(2))
				t.o.NextTerm(context.Background())

				removed := append(t.spyCommunicators["worker-0"].removed,
					append(t.spyCommunicators["worker-1"].removed,
						t.spyCommunicators["worker-2"].removed...)...)
				Expect(t, removed).To(HaveLen(1))
			})

			o.Group("with single worker", func() {
				o.Spec("it only assigns the task once", func(t TO) {
					communicator := newSpyCommunicator()
					t.spyCommunicators["worker"] = communicator
					t.o.UpdateWorkers([]orchestrator.Worker{{Identifier: "worker", Communicator: communicator}})
					t.spyCommunicators["worker"].actual = []interface{}{"multi-task"}
					t.o.AddTask("multi-task", orchestrator.WithTaskInstances(2))

					t.o.NextTerm(context.Background())

					Expect(t, t.spyCommunicators["worker"].added).To(HaveLen(0))
				})
			})

			o.Group("with more total tasks than workers", func() {
				o.Spec("it assigns each task to 2 different workers", func(t TO) {
					t.o.AddTask("multi-task-0", orchestrator.WithTaskInstances(2))
					t.o.AddTask("multi-task-1", orchestrator.WithTaskInstances(2))
					t.o.AddTask("multi-task-2", orchestrator.WithTaskInstances(2))
					t.o.NextTerm(context.Background())

					Expect(t, count("multi-task-0", append(append(
						t.spyCommunicators["worker-0"].added,
						t.spyCommunicators["worker-1"].added...),
						t.spyCommunicators["worker-2"].added...,
					))).To(Equal(2))

					Expect(t, count("multi-task-1", append(append(
						t.spyCommunicators["worker-0"].added,
						t.spyCommunicators["worker-1"].added...),
						t.spyCommunicators["worker-2"].added...,
					))).To(Equal(2))

					Expect(t, count("multi-task-2", append(append(
						t.spyCommunicators["worker-0"].added,
						t.spyCommunicators["worker-1"].added...),
						t.spyCommunicators["worker-2"].added...,
					))).To(Equal(2))
				})
			})
		})

		o.Group("with one task assigned", func() {
			o.BeforeEach(func(t TO) TO {
				t.spyCommunicators["worker-0"].actual = []interface{}{"task-1"}
				return t
			})

			o.Spec("it does not replace the existing task", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spyCommunicators["worker-0"].added).To(HaveLen(0))
				Expect(t, t.spyCommunicators["worker-1"].added).To(HaveLen(1))
				Expect(t, t.spyCommunicators["worker-2"].added).To(HaveLen(1))

				var all []interface{}
				for _, spy := range t.spyCommunicators {
					all = append(all, spy.added...)
				}

				Expect(t, all).To(Contain("task-0", "task-2"))
			})
		})

		o.Group("with extra task", func() {
			o.BeforeEach(func(t TO) TO {
				t.spyCommunicators["worker-0"].actual = []interface{}{"extra"}
				return t
			})

			o.Spec("it removes the task", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spyCommunicators["worker-0"].removed).To(HaveLen(1))
				Expect(t, t.spyCommunicators["worker-1"].removed).To(HaveLen(0))
				Expect(t, t.spyCommunicators["worker-2"].removed).To(HaveLen(0))

				Expect(t, t.spyCommunicators["worker-0"].removed).To(Contain("extra"))

				ctx := t.spyCommunicators["worker-0"].removedCtx[0]
				_, ok := ctx.Deadline()
				Expect(t, ok).To(BeTrue())
			})
		})

		o.Group("with too many of a task", func() {
			o.BeforeEach(func(t TO) TO {
				t.spyCommunicators["worker-0"].actual = []interface{}{"task-0"}
				t.spyCommunicators["worker-1"].actual = []interface{}{"task-0"}
				return t
			})

			o.Spec("it removes the task and adds the required", func(t TO) {
				t.o.NextTerm(context.Background())

				removed := append(t.spyCommunicators["worker-0"].removed,
					append(t.spyCommunicators["worker-1"].removed,
						t.spyCommunicators["worker-2"].removed...)...)
				Expect(t, removed).To(HaveLen(1))

				added := append(t.spyCommunicators["worker-0"].added,
					append(t.spyCommunicators["worker-1"].added,
						t.spyCommunicators["worker-2"].added...)...)
				Expect(t, added).To(HaveLen(2))
			})
		})

		o.Group("with a worker removed", func() {
			o.BeforeEach(func(t TO) TO {
				t.spyCommunicators["worker-0"].actual = []interface{}{"task-0"}
				t.spyCommunicators["worker-2"].actual = []interface{}{"task-2"}

				t.o.RemoveWorker(orchestrator.Worker{Identifier: "worker-1"})
				return t
			})

			o.Spec("divvys up work amongst the remaining workers", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spyCommunicators["worker-1"].added).To(HaveLen(0))
				Expect(t, append(
					t.spyCommunicators["worker-0"].added,
					t.spyCommunicators["worker-2"].added...,
				)).To(HaveLen(1))

				Expect(t, append(
					t.spyCommunicators["worker-0"].added,
					t.spyCommunicators["worker-2"].added...,
				)).To(Contain(
					"task-1",
				))
			})
		})

		o.Group("with a worker who is empty", func() {
			o.BeforeEach(func(t TO) TO {
				t.spyCommunicators["worker-0"].actual = []interface{}{"task-0", "task-1"}
				t.spyCommunicators["worker-2"].actual = []interface{}{"task-2"}
				return t
			})

			o.Spec("evens out the tasks amongst the workers", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spyCommunicators["worker-1"].added).To(HaveLen(1))
				Expect(t, t.spyCommunicators["worker-0"].removed).To(HaveLen(1))
			})
		})

		o.Group("with number of tasks doesn't cleanly fit in", func() {
			o.BeforeEach(func(t TO) TO {
				t.o.AddTask("task-3")
				t.spyCommunicators["worker-0"].actual = []interface{}{"task-0", "task-3"}
				t.spyCommunicators["worker-1"].actual = []interface{}{"task-1"}
				t.spyCommunicators["worker-2"].actual = []interface{}{"task-2"}
				return t
			})

			o.Spec("doesn't move anything around", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spyCommunicators["worker-0"].removed).To(HaveLen(0))
			})
		})

		o.Group("with a workers updated", func() {
			o.BeforeEach(func(t TO) TO {
				t.spyCommunicators["worker-0"].actual = []interface{}{"task-0"}
				t.spyCommunicators["worker-2"].actual = []interface{}{"task-2"}

				t.o.UpdateWorkers([]orchestrator.Worker{
					{Identifier: "worker-0", Communicator: t.spyCommunicators["worker-0"]},
					{Identifier: "worker-2", Communicator: t.spyCommunicators["worker-2"]},
				})
				return t
			})

			o.Spec("divvys up work amongst the remaining workers", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spyCommunicators["worker-1"].added).To(HaveLen(0))
				Expect(t, append(
					t.spyCommunicators["worker-0"].added,
					t.spyCommunicators["worker-2"].added...,
				)).To(HaveLen(1))

				Expect(t, append(
					t.spyCommunicators["worker-0"].added,
					t.spyCommunicators["worker-2"].added...,
				)).To(Contain(
					"task-1",
				))
			})
		})

		o.Group("with a task removed", func() {
			o.BeforeEach(func(t TO) TO {
				t.spyCommunicators["worker-0"].actual = []interface{}{"task-0"}
				t.spyCommunicators["worker-1"].actual = []interface{}{"task-1"}
				t.spyCommunicators["worker-2"].actual = []interface{}{"task-2"}

				t.o.RemoveTask("task-1")
				return t
			})

			o.Spec("removes the tasks from the worker", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spyCommunicators["worker-1"].removed).To(HaveLen(1))
				Expect(t, t.spyCommunicators["worker-1"].removed).To(Contain("task-1"))

				Expect(t, append(append(
					t.spyCommunicators["worker-0"].added,
					t.spyCommunicators["worker-1"].added...),
					t.spyCommunicators["worker-2"].added...,
				)).To(HaveLen(0))
			})
		})

		o.Group("with task list updated", func() {
			o.BeforeEach(func(t TO) TO {
				t.spyCommunicators["worker-0"].actual = []interface{}{"task-0"}
				t.spyCommunicators["worker-1"].actual = []interface{}{"task-1"}
				t.spyCommunicators["worker-2"].actual = []interface{}{"task-2"}

				t.o.UpdateTasks([]orchestrator.Task{
					{
						Definition: "task-0",
						Instances:  1,
					},
					{
						Definition: "task-2",
						Instances:  1,
					},
				})
				return t
			})

			o.Spec("removes the tasks from the worker", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spyCommunicators["worker-1"].removed).To(HaveLen(1))
				Expect(t, t.spyCommunicators["worker-1"].removed).To(Contain("task-1"))

				Expect(t, append(append(
					t.spyCommunicators["worker-0"].added,
					t.spyCommunicators["worker-1"].added...),
					t.spyCommunicators["worker-2"].added...,
				)).To(HaveLen(0))
			})
		})

		o.Group("with a worker returning an error for list", func() {
			o.BeforeEach(func(t TO) TO {
				t.spyCommunicators["worker-1"].listErrs = errors.New("some-error")

				return t
			})

			o.Spec("divvys up work amongst the remaining workers", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spyCommunicators["worker-1"].added).To(HaveLen(0))
				Expect(t, append(
					t.spyCommunicators["worker-0"].added,
					t.spyCommunicators["worker-2"].added...,
				)).To(HaveLen(3))

				Expect(t, append(
					t.spyCommunicators["worker-0"].added,
					t.spyCommunicators["worker-2"].added...,
				)).To(Contain(
					"task-0",
					"task-1",
					"task-2",
				))
			})
		})

		o.Group("with a worker blocking trying to list", func() {
			o.BeforeEach(func(t TO) TO {
				t.spyCommunicators["worker-1"].block = true

				return t
			})

			o.Spec("divvys up work amongst the remaining workers", func(t TO) {
				t.o.NextTerm(context.Background())

				Expect(t, t.spyCommunicators["worker-1"].added).To(HaveLen(0))
				Expect(t, append(
					t.spyCommunicators["worker-0"].added,
					t.spyCommunicators["worker-2"].added...,
				)).To(HaveLen(3))

				Expect(t, append(
					t.spyCommunicators["worker-0"].added,
					t.spyCommunicators["worker-2"].added...,
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

	block      bool
	listErrs   error
	actual     []interface{}
	added      []interface{}
	removed    []interface{}
	listCtx    []context.Context
	addedCtx   []context.Context
	removedCtx []context.Context
}

func newSpyCommunicator() *spyCommunicator {
	return &spyCommunicator{}
}

func (s *spyCommunicator) List(ctx context.Context) ([]interface{}, error) {
	if s.block {
		var c chan int
		<-c
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.listCtx = append(s.listCtx, ctx)
	return s.actual, s.listErrs
}

func (s *spyCommunicator) Add(ctx context.Context, task interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.added = append(s.added, task)
	s.addedCtx = append(s.addedCtx, ctx)
	return nil
}

func (s *spyCommunicator) Remove(ctx context.Context, task interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.removed = append(s.removed, task)
	s.removedCtx = append(s.removedCtx, ctx)
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
