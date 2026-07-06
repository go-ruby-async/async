package async

import (
	"errors"
	"testing"
	"time"
)

// --- Barrier ---

func TestBarrierWaitAll(t *testing.T) {
	var done []int
	Run(func(task *Task) (any, error) {
		b := NewBarrier()
		if !b.Empty() {
			t.Errorf("new barrier not empty")
		}
		for i := 0; i < 3; i++ {
			i := i
			b.Async(task, func(ct *Task) (any, error) {
				ct.Sleep(time.Duration(i+1) * time.Millisecond)
				done = append(done, i)
				return nil, nil
			})
		}
		if b.Size() != 3 {
			t.Errorf("size = %d", b.Size())
		}
		err := b.Wait(task)
		if err != nil {
			t.Errorf("wait err = %v", err)
		}
		if !b.Empty() {
			t.Errorf("barrier not drained")
		}
		return nil, nil
	})
	if len(done) != 3 {
		t.Fatalf("done = %v", done)
	}
}

func TestBarrierWaitPropagatesFailure(t *testing.T) {
	boom := errors.New("task boom")
	Run(func(task *Task) (any, error) {
		b := NewBarrier()
		b.Async(task, func(ct *Task) (any, error) { return nil, boom })
		b.Async(task, func(ct *Task) (any, error) { return nil, nil })
		err := b.Wait(task)
		if !errors.Is(err, boom) {
			t.Errorf("err = %v", err)
		}
		return nil, nil
	})
}

func TestBarrierStop(t *testing.T) {
	Run(func(task *Task) (any, error) {
		b := NewBarrier()
		var tasks []*Task
		for i := 0; i < 2; i++ {
			tasks = append(tasks, b.Async(task, func(ct *Task) (any, error) {
				ct.Sleep(time.Hour)
				return nil, nil
			}))
		}
		task.Yield()
		b.Stop()
		if !b.Empty() {
			t.Errorf("barrier not cleared after stop")
		}
		for _, x := range tasks {
			x.Wait(task)
			if !x.StoppedQ() {
				t.Errorf("task not stopped: %v", x.State())
			}
		}
		return nil, nil
	})
}

// --- Semaphore ---

func TestSemaphoreMutualExclusion(t *testing.T) {
	var active, maxActive int
	Run(func(task *Task) (any, error) {
		sem := NewSemaphore(2)
		if sem.Limit() != 2 {
			t.Errorf("limit = %d", sem.Limit())
		}
		b := NewBarrier()
		for i := 0; i < 5; i++ {
			b.Async(task, func(ct *Task) (any, error) {
				sem.Acquire(ct)
				active++
				if active > maxActive {
					maxActive = active
				}
				ct.Yield()
				active--
				sem.Release()
				return nil, nil
			})
		}
		return nil, b.Wait(task)
	})
	if maxActive != 2 {
		t.Fatalf("maxActive = %d, want 2", maxActive)
	}
}

func TestSemaphoreClampAndAcquireDo(t *testing.T) {
	Run(func(task *Task) (any, error) {
		sem := NewSemaphore(0) // clamped to 1
		if sem.Limit() != 1 {
			t.Errorf("limit = %d", sem.Limit())
		}
		res, err := sem.AcquireDo(task, func() (any, error) {
			if sem.Count() != 1 {
				t.Errorf("count = %d", sem.Count())
			}
			if !sem.Blocking() {
				t.Errorf("expected blocking at limit")
			}
			return "held", nil
		})
		if err != nil || res != "held" {
			t.Errorf("got %v, %v", res, err)
		}
		if sem.Count() != 0 {
			t.Errorf("count after release = %d", sem.Count())
		}
		return nil, nil
	})
}

func TestSemaphoreReleaseWithoutAcquire(t *testing.T) {
	Run(func(task *Task) (any, error) {
		sem := NewSemaphore(1)
		sem.Release() // count already 0, no waiters: no-op
		if sem.Count() != 0 {
			t.Errorf("count = %d", sem.Count())
		}
		return nil, nil
	})
}

func TestSemaphoreSetLimitWakesWaiters(t *testing.T) {
	var got []int
	Run(func(task *Task) (any, error) {
		sem := NewSemaphore(1)
		sem.Acquire(task) // hold the only permit
		b := NewBarrier()
		for i := 0; i < 2; i++ {
			i := i
			b.Async(task, func(ct *Task) (any, error) {
				sem.Acquire(ct)
				got = append(got, i)
				return nil, nil
			})
		}
		task.Yield() // both block
		if sem.Waiting() != 2 {
			t.Errorf("waiting = %d", sem.Waiting())
		}
		sem.SetLimit(0) // clamped to 1: no headroom, no wake
		sem.SetLimit(3) // headroom for both waiters -> wakes them
		return nil, b.Wait(task)
	})
	if len(got) != 2 {
		t.Fatalf("got = %v", got)
	}
}

func TestSemaphoreAcquireCancelled(t *testing.T) {
	Run(func(task *Task) (any, error) {
		sem := NewSemaphore(1)
		sem.Acquire(task)
		c := task.Async(func(ct *Task) (any, error) {
			sem.Acquire(ct) // blocks; task holds permit
			return nil, nil
		})
		task.Yield()
		if sem.Waiting() != 1 {
			t.Errorf("waiting = %d", sem.Waiting())
		}
		c.Stop() // cancelled while queued -> removeWaiter
		c.Wait(task)
		if sem.Waiting() != 0 {
			t.Errorf("waiter not removed: %d", sem.Waiting())
		}
		return nil, nil
	})
}

// --- Condition & Notification ---

func TestConditionSignal(t *testing.T) {
	var got []any
	Run(func(task *Task) (any, error) {
		cond := NewCondition()
		if !cond.Empty() {
			t.Errorf("new condition not empty")
		}
		b := NewBarrier()
		for i := 0; i < 2; i++ {
			b.Async(task, func(ct *Task) (any, error) {
				got = append(got, cond.Wait(ct))
				return nil, nil
			})
		}
		task.Yield()
		if cond.WaitCount() != 2 {
			t.Errorf("wait count = %d", cond.WaitCount())
		}
		cond.Signal("hello")
		return nil, b.Wait(task)
	})
	if len(got) != 2 || got[0] != "hello" || got[1] != "hello" {
		t.Fatalf("got = %v", got)
	}
}

func TestConditionWaitCancelled(t *testing.T) {
	Run(func(task *Task) (any, error) {
		cond := NewCondition()
		c := task.Async(func(ct *Task) (any, error) {
			cond.Wait(ct)
			return nil, nil
		})
		task.Yield()
		c.Stop() // cancelled while waiting -> remove
		c.Wait(task)
		if !cond.Empty() {
			t.Errorf("waiter not removed")
		}
		return nil, nil
	})
}

func TestNotification(t *testing.T) {
	var woke bool
	Run(func(task *Task) (any, error) {
		n := NewNotification()
		c := task.Async(func(ct *Task) (any, error) {
			n.Wait(ct)
			woke = true
			return nil, nil
		})
		task.Yield()
		if n.WaitCount() != 1 {
			t.Errorf("wait count = %d", n.WaitCount())
		}
		n.Signal()
		_, err := c.Wait(task)
		return nil, err
	})
	if !woke {
		t.Fatalf("notification did not wake waiter")
	}
}

// --- Queue ---

func TestQueueProducerConsumer(t *testing.T) {
	var got []any
	Run(func(task *Task) (any, error) {
		q := NewQueue()
		if !q.Empty() {
			t.Errorf("new queue not empty")
		}
		consumer := task.Async(func(ct *Task) (any, error) {
			for i := 0; i < 3; i++ {
				got = append(got, q.Dequeue(ct)) // blocks until produced
			}
			return nil, nil
		})
		task.Yield() // consumer blocks on empty queue
		q.Push(1)
		q.Push(2)
		q.Enqueue(3)
		_, err := consumer.Wait(task)
		return nil, err
	})
	if len(got) != 3 {
		t.Fatalf("got = %v", got)
	}
}

func TestQueueBufferedFirst(t *testing.T) {
	Run(func(task *Task) (any, error) {
		q := NewQueue()
		q.Enqueue("a") // no waiter yet: just buffered
		if q.Size() != 1 {
			t.Errorf("size = %d", q.Size())
		}
		if v := q.Pop(task); v != "a" { // item present: no block
			t.Errorf("pop = %v", v)
		}
		return nil, nil
	})
}

func TestQueueDequeueCancelled(t *testing.T) {
	Run(func(task *Task) (any, error) {
		q := NewQueue()
		c := task.Async(func(ct *Task) (any, error) {
			q.Dequeue(ct)
			return nil, nil
		})
		task.Yield()
		c.Stop() // cancelled while waiting -> removeWaiter
		c.Wait(task)
		q.Enqueue("orphan") // no waiter to wake
		if q.Size() != 1 {
			t.Errorf("size = %d", q.Size())
		}
		return nil, nil
	})
}

// --- LimitedQueue ---

func TestLimitedQueueBackpressure(t *testing.T) {
	var produced, consumed []int
	Run(func(task *Task) (any, error) {
		q := NewLimitedQueue(0) // clamped to 1
		if q.Limit() != 1 {
			t.Errorf("limit = %d", q.Limit())
		}
		producer := task.Async(func(ct *Task) (any, error) {
			for i := 0; i < 3; i++ {
				q.Enqueue(ct, i) // blocks when full
				produced = append(produced, i)
			}
			return nil, nil
		})
		task.Yield()
		if !q.LimitedQ() {
			t.Errorf("queue should be at capacity")
		}
		consumer := task.Async(func(ct *Task) (any, error) {
			for i := 0; i < 3; i++ {
				consumed = append(consumed, q.Dequeue(ct).(int))
			}
			return nil, nil
		})
		if _, err := producer.Wait(task); err != nil {
			return nil, err
		}
		_, err := consumer.Wait(task)
		return nil, err
	})
	if len(produced) != 3 || len(consumed) != 3 {
		t.Fatalf("produced %v consumed %v", produced, consumed)
	}
}

func TestLimitedQueueDequeueBlocks(t *testing.T) {
	Run(func(task *Task) (any, error) {
		q := NewLimitedQueue(2)
		c := task.Async(func(ct *Task) (any, error) {
			return q.Dequeue(ct), nil // blocks on empty
		})
		task.Yield()
		if !q.Empty() {
			t.Errorf("expected empty")
		}
		q.Enqueue(task, "x") // wakes consumer; no producer waiting
		res, err := c.Wait(task)
		if err != nil || res != "x" {
			t.Errorf("got %v, %v", res, err)
		}
		if q.Size() != 0 {
			t.Errorf("size = %d", q.Size())
		}
		return nil, nil
	})
}

func TestLimitedQueueEnqueueCancelled(t *testing.T) {
	Run(func(task *Task) (any, error) {
		q := NewLimitedQueue(1)
		q.Enqueue(task, "full")
		c := task.Async(func(ct *Task) (any, error) {
			q.Enqueue(ct, "blocked") // queue full -> blocks
			return nil, nil
		})
		task.Yield()
		c.Stop() // cancelled while blocked -> removeEnq
		c.Wait(task)
		return nil, nil
	})
}

func TestLimitedQueueDequeueCancelled(t *testing.T) {
	Run(func(task *Task) (any, error) {
		q := NewLimitedQueue(1)
		c := task.Async(func(ct *Task) (any, error) {
			q.Dequeue(ct) // empty -> blocks
			return nil, nil
		})
		task.Yield()
		c.Stop() // cancelled while blocked -> removeDeq
		c.Wait(task)
		return nil, nil
	})
}

// --- Waiter ---

func TestWaiter(t *testing.T) {
	Run(func(task *Task) (any, error) {
		w := NewWaiter(task)
		for i := 0; i < 3; i++ {
			i := i
			w.Async(func(ct *Task) (any, error) {
				ct.Sleep(time.Duration(i+1) * time.Millisecond)
				return i * 10, nil
			})
		}
		if w.Size() != 3 {
			t.Errorf("size = %d", w.Size())
		}
		// Wait for the first two.
		res, err := w.Wait(task, 2)
		if err != nil || len(res) != 2 || res[0] != 0 || res[1] != 10 {
			t.Errorf("partial wait: %v, %v", res, err)
		}
		// Wait for all (count clamped from a large value).
		all, err := w.Wait(task, 99)
		if err != nil || len(all) != 3 {
			t.Errorf("full wait: %v, %v", all, err)
		}
		return nil, nil
	})
}

func TestWaiterNegativeCountAndFailure(t *testing.T) {
	boom := errors.New("waiter boom")
	Run(func(task *Task) (any, error) {
		w := NewWaiter(task)
		w.Async(func(ct *Task) (any, error) { return 1, nil })
		w.Async(func(ct *Task) (any, error) { return nil, boom })
		res, err := w.Wait(task, -1) // negative -> all
		if !errors.Is(err, boom) {
			t.Errorf("err = %v", err)
		}
		if len(res) != 1 || res[0] != 1 {
			t.Errorf("partial results = %v", res)
		}
		return nil, nil
	})
}
