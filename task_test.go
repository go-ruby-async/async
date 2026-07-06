package async

import (
	"errors"
	"testing"
	"time"
)

func TestRunResult(t *testing.T) {
	res, err := Run(func(task *Task) (any, error) { return 42, nil })
	if err != nil || res != 42 {
		t.Fatalf("got %v, %v", res, err)
	}
}

func TestRunOnExplicitScheduler(t *testing.T) {
	s := NewScheduler()
	res, err := RunOn(s, func(task *Task) (any, error) {
		if task.Scheduler() != s {
			t.Errorf("scheduler mismatch")
		}
		return "ok", nil
	})
	if err != nil || res != "ok" {
		t.Fatalf("got %v, %v", res, err)
	}
}

func TestRootFailure(t *testing.T) {
	boom := errors.New("boom")
	res, err := Run(func(task *Task) (any, error) { return nil, boom })
	if res != nil || !errors.Is(err, boom) {
		t.Fatalf("got %v, %v", res, err)
	}
}

func TestRootSelfStop(t *testing.T) {
	res, err := Run(func(task *Task) (any, error) {
		task.Stop()
		return "unreachable", nil
	})
	if res != nil || !errors.Is(err, ErrStop) {
		t.Fatalf("got %v, %v", res, err)
	}
}

func TestRootPanicString(t *testing.T) {
	_, err := Run(func(task *Task) (any, error) {
		panic("kaboom")
	})
	if err == nil || err.Error() != "async: task panicked: kaboom" {
		t.Fatalf("err = %v", err)
	}
}

func TestRootPanicError(t *testing.T) {
	sentinel := errors.New("panicked-error")
	_, err := Run(func(task *Task) (any, error) {
		panic(sentinel)
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v", err)
	}
}

func TestChildWaitResult(t *testing.T) {
	res, err := Run(func(task *Task) (any, error) {
		c := task.Async(func(ct *Task) (any, error) {
			ct.Sleep(5 * time.Millisecond)
			return "child", nil
		})
		return c.Wait(task)
	})
	if err != nil || res != "child" {
		t.Fatalf("got %v, %v", res, err)
	}
}

func TestChildWaitAlreadyDone(t *testing.T) {
	Run(func(task *Task) (any, error) {
		c := task.Async(func(ct *Task) (any, error) { return 7, nil })
		task.Yield() // let c finish
		if !c.CompleteQ() {
			t.Errorf("child not complete: %v", c.State())
		}
		res, err := c.Wait(task) // already done: no suspend
		if err != nil || res != 7 {
			t.Errorf("got %v, %v", res, err)
		}
		return nil, nil
	})
}

func TestChildFailureViaWait(t *testing.T) {
	boom := errors.New("child boom")
	_, err := Run(func(task *Task) (any, error) {
		c := task.Async(func(ct *Task) (any, error) { return nil, boom })
		return c.Wait(task)
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestStopInitializedChild(t *testing.T) {
	var ran bool
	Run(func(task *Task) (any, error) {
		c := task.Async(func(ct *Task) (any, error) {
			ran = true
			return nil, nil
		})
		c.Stop() // stopped before it ever runs
		_, err := c.Wait(task)
		if !errors.Is(err, ErrStop) {
			t.Errorf("wait err = %v", err)
		}
		if !c.StoppedQ() {
			t.Errorf("state = %v", c.State())
		}
		return nil, nil
	})
	if ran {
		t.Fatalf("body of a pre-stopped task ran")
	}
}

func TestStopSuspendedChild(t *testing.T) {
	Run(func(task *Task) (any, error) {
		c := task.Async(func(ct *Task) (any, error) {
			ct.Sleep(time.Hour)
			return "never", nil
		})
		task.Yield()
		c.Stop()
		_, err := c.Wait(task)
		if !errors.Is(err, ErrStop) {
			t.Errorf("err = %v", err)
		}
		if !c.StoppedQ() {
			t.Errorf("state = %v", c.State())
		}
		return nil, nil
	})
}

func TestStopAlreadyDone(t *testing.T) {
	Run(func(task *Task) (any, error) {
		c := task.Async(func(ct *Task) (any, error) { return 1, nil })
		task.Yield()
		c.Stop() // no-op on a completed task
		if !c.CompleteQ() {
			t.Errorf("state = %v", c.State())
		}
		return nil, nil
	})
}

func TestFailurePropagatesToChildren(t *testing.T) {
	var grand *Task
	boom := errors.New("parent boom")
	Run(func(task *Task) (any, error) {
		parent := task.Async(func(pt *Task) (any, error) {
			grand = pt.Async(func(gt *Task) (any, error) {
				gt.Sleep(time.Hour)
				return "never", nil
			})
			pt.Yield() // let the grandchild block
			return nil, boom
		})
		_, err := parent.Wait(task)
		if !errors.Is(err, boom) {
			t.Errorf("err = %v", err)
		}
		grand.Wait(task)
		return nil, nil
	})
	if !grand.StoppedQ() {
		t.Fatalf("grandchild state = %v", grand.State())
	}
}

func TestTeardownStopsBlockedTask(t *testing.T) {
	var blocked *Task
	Run(func(task *Task) (any, error) {
		cond := NewCondition()
		blocked = task.Async(func(ct *Task) (any, error) {
			cond.Wait(ct) // never signalled
			return "never", nil
		})
		task.Yield() // let it block; root then completes
		return nil, nil
	})
	if !blocked.StoppedQ() {
		t.Fatalf("blocked task not torn down: %v", blocked.State())
	}
}

func TestWaitingTaskStopped(t *testing.T) {
	// A task blocked in Wait on another task is itself stopped: it must drop out
	// of the awaited task's waiter list.
	Run(func(task *Task) (any, error) {
		long := task.Async(func(lt *Task) (any, error) {
			lt.Sleep(time.Hour)
			return nil, nil
		})
		waiter := task.Async(func(wt *Task) (any, error) {
			return long.Wait(wt) // blocks on long
		})
		task.Yield() // let waiter block on long
		waiter.Stop()
		waiter.Wait(task)
		long.Stop()
		long.Wait(task)
		if !waiter.StoppedQ() {
			t.Errorf("waiter state = %v", waiter.State())
		}
		return nil, nil
	})
}

func TestWithTimeoutFires(t *testing.T) {
	res, err := Run(func(task *Task) (any, error) {
		return task.WithTimeout(10*time.Millisecond, func(ct *Task) (any, error) {
			ct.Sleep(time.Hour)
			return "done", nil
		})
	})
	if res != nil || !errors.Is(err, ErrTimeout) {
		t.Fatalf("got %v, %v", res, err)
	}
}

func TestWithTimeoutSucceeds(t *testing.T) {
	res, err := Run(func(task *Task) (any, error) {
		return task.WithTimeout(time.Hour, func(ct *Task) (any, error) {
			return "quick", nil
		})
	})
	if err != nil || res != "quick" {
		t.Fatalf("got %v, %v", res, err)
	}
}

func TestAccessors(t *testing.T) {
	Run(func(task *Task) (any, error) {
		if task.Parent() != nil {
			t.Errorf("root parent should be nil")
		}
		c := task.Async(func(ct *Task) (any, error) { return 9, nil })
		if c.Parent() != task {
			t.Errorf("child parent mismatch")
		}
		if len(task.Children()) != 1 {
			t.Errorf("children = %v", task.Children())
		}
		if !c.RunningQ() && c.State() != Initialized {
			t.Errorf("unexpected state %v", c.State())
		}
		task.Yield()
		if c.Result() != 9 || c.Err() != nil {
			t.Errorf("result %v err %v", c.Result(), c.Err())
		}
		if !c.CompleteQ() || c.FailedQ() || c.StoppedQ() {
			t.Errorf("predicate mismatch")
		}
		return nil, nil
	})
}

func TestStopCascadesToChildren(t *testing.T) {
	var grand *Task
	Run(func(task *Task) (any, error) {
		childA := task.Async(func(at *Task) (any, error) {
			grand = at.Async(func(bt *Task) (any, error) {
				bt.Sleep(time.Hour)
				return nil, nil
			})
			at.Sleep(time.Hour)
			return nil, nil
		})
		task.Yield()  // let childA spawn grand and suspend
		childA.Stop() // childA has a live child: Stop cascades to grand
		childA.Wait(task)
		grand.Wait(task)
		return nil, nil
	})
	if !grand.StoppedQ() {
		t.Fatalf("grandchild not stopped: %v", grand.State())
	}
}

func TestResultOfIncomplete(t *testing.T) {
	Run(func(task *Task) (any, error) {
		c := task.Async(func(ct *Task) (any, error) {
			ct.Sleep(time.Hour)
			return 1, nil
		})
		task.Yield()
		if c.Result() != nil {
			t.Errorf("incomplete Result = %v", c.Result())
		}
		c.Stop()
		c.Wait(task)
		return nil, nil
	})
}
