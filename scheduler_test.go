package async

import (
	"testing"
	"time"
)

func TestSchedulerVirtualClock(t *testing.T) {
	var order []string
	s := NewScheduler()
	RunOn(s, func(task *Task) (any, error) {
		b := NewBarrier()
		b.Async(task, func(ct *Task) (any, error) {
			ct.Sleep(20 * time.Millisecond)
			order = append(order, "late")
			return nil, nil
		})
		b.Async(task, func(ct *Task) (any, error) {
			ct.Sleep(10 * time.Millisecond)
			order = append(order, "early")
			return nil, nil
		})
		return nil, b.Wait(task)
	})
	if len(order) != 2 || order[0] != "early" || order[1] != "late" {
		t.Fatalf("order = %v", order)
	}
	if s.Now() != 20*time.Millisecond {
		t.Fatalf("virtual clock = %v", s.Now())
	}
}

func TestSchedulerYieldOrdering(t *testing.T) {
	var log []int
	Run(func(task *Task) (any, error) {
		b := NewBarrier()
		for i := 0; i < 3; i++ {
			i := i
			b.Async(task, func(ct *Task) (any, error) {
				log = append(log, i)
				ct.Yield()
				log = append(log, 10+i)
				return nil, nil
			})
		}
		return nil, b.Wait(task)
	})
	// All first-halves run before any second-half, due to cooperative yielding.
	want := []int{0, 1, 2, 10, 11, 12}
	if len(log) != len(want) {
		t.Fatalf("log = %v", log)
	}
	for i := range want {
		if log[i] != want[i] {
			t.Fatalf("log = %v want %v", log, want)
		}
	}
}
