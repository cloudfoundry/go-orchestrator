package orchestrate_test

import (
	"context"
	"fmt"
	"testing"

	orchestrate "github.com/apoydence/go-orchestrate"
	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
)

type TO struct {
	*testing.T
	spy *spyCommunicator
	o   *orchestrate.Orchestrator
}

func TestOrchestrator(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TO {
		spy := newSpyCommunicator()
		return TO{
			T:   t,
			o:   orchestrate.New(spy),
			spy: spy,
		}
	})

	o.Group("with 3 worker nodes and 3 tasks", func() {
		o.BeforeEach(func(t TO) TO {
			for i := 0; i < 3; i++ {
				t.o.AddWorker(fmt.Sprintf("worker-%d", i))
				t.o.AddTask(fmt.Sprintf("task-%d", i))
			}
			return t
		})

		o.Group("no tasks yet assigned", func() {
			o.Spec("it evenly splits tasks among the cluster", func(t TO) {
				t.o.NextTerm(context.TODO())

				Expect(t, t.spy.added).To(HaveLen(3))

				Expect(t, t.spy.added["worker-0"]).To(HaveLen(1))
				Expect(t, t.spy.added["worker-1"]).To(HaveLen(1))
				Expect(t, t.spy.added["worker-2"]).To(HaveLen(1))

				var all []string
				for _, tasks := range t.spy.added {
					all = append(all, tasks...)
				}

				Expect(t, all).To(Contain("task-0", "task-1", "task-2"))
			})
		})

		o.Group("one task assigned", func() {
			o.BeforeEach(func(t TO) TO {
				t.spy.actual["worker-0"] = []string{"task-1"}
				return t
			})

			o.Spec("it does not replace the existing task", func(t TO) {
				t.o.NextTerm(context.TODO())

				Expect(t, t.spy.added).To(HaveLen(2))

				Expect(t, t.spy.added["worker-0"]).To(HaveLen(0))
				Expect(t, t.spy.added["worker-1"]).To(HaveLen(1))
				Expect(t, t.spy.added["worker-2"]).To(HaveLen(1))

				var all []string
				for _, tasks := range t.spy.added {
					all = append(all, tasks...)
				}

				Expect(t, all).To(Contain("task-0", "task-2"))
			})
		})

		o.Group("extra task", func() {
			o.BeforeEach(func(t TO) TO {
				t.spy.actual["worker-0"] = []string{"extra"}
				return t
			})

			o.Spec("it removes the task", func(t TO) {
				t.o.NextTerm(context.TODO())

				Expect(t, t.spy.added).To(HaveLen(3))
				Expect(t, t.spy.removed).To(HaveLen(1))

				Expect(t, t.spy.removed["worker-0"]).To(HaveLen(1))
				Expect(t, t.spy.removed["worker-1"]).To(HaveLen(0))
				Expect(t, t.spy.removed["worker-2"]).To(HaveLen(0))

				Expect(t, t.spy.removed["worker-0"]).To(Contain("extra"))
			})
		})

		o.Group("too many of a task", func() {
			o.BeforeEach(func(t TO) TO {
				t.spy.actual["worker-0"] = []string{"task-0"}
				t.spy.actual["worker-1"] = []string{"task-0"}
				return t
			})

			o.Spec("it removes the task and adds the required", func(t TO) {
				t.o.NextTerm(context.TODO())

				Expect(t, t.spy.removed).To(HaveLen(1))
				Expect(t, t.spy.added).To(HaveLen(2))
			})
		})
	})
}

type spyCommunicator struct {
	actual  map[string][]string
	added   map[string][]string
	removed map[string][]string
}

func newSpyCommunicator() *spyCommunicator {
	return &spyCommunicator{
		actual:  make(map[string][]string),
		added:   make(map[string][]string),
		removed: make(map[string][]string),
	}
}

func (s *spyCommunicator) List(ctx context.Context, worker string) ([]string, error) {
	return s.actual[worker], nil
}

func (s *spyCommunicator) Add(ctx context.Context, worker string, task string) error {
	s.added[worker] = append(s.added[worker], task)
	return nil
}

func (s *spyCommunicator) Remove(ctx context.Context, worker string, task string) error {
	s.removed[worker] = append(s.removed[worker], task)
	return nil
}
