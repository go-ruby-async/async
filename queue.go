package async

// Queue is an unbounded FIFO channel between tasks, mirroring Async::Queue. A
// task dequeuing an empty queue suspends its fiber until an item is enqueued.
type Queue struct {
	items   []any
	waiters []*Task
}

// NewQueue returns an empty queue.
func NewQueue() *Queue { return &Queue{} }

// Enqueue appends an item and wakes the longest-waiting dequeuer, if any
// (Async::Queue#enqueue / #push / #<<).
func (q *Queue) Enqueue(v any) {
	q.items = append(q.items, v)
	if len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		w.wake()
	}
}

// Dequeue removes and returns the head item, suspending the calling task while
// the queue is empty (Async::Queue#dequeue / #pop).
func (q *Queue) Dequeue(t *Task) any {
	for len(q.items) == 0 {
		q.waiters = append(q.waiters, t)
		t.suspend(func() { q.removeWaiter(t) })
	}
	v := q.items[0]
	q.items = q.items[1:]
	return v
}

// Push is an alias for Enqueue (Async::Queue#push).
func (q *Queue) Push(v any) { q.Enqueue(v) }

// Pop is an alias for Dequeue (Async::Queue#pop).
func (q *Queue) Pop(t *Task) any { return q.Dequeue(t) }

// Size returns the number of buffered items (Async::Queue#size).
func (q *Queue) Size() int { return len(q.items) }

// Empty reports whether the queue holds no items (Async::Queue#empty?).
func (q *Queue) Empty() bool { return len(q.items) == 0 }

func (q *Queue) removeWaiter(t *Task) {
	for i, x := range q.waiters {
		if x == t {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			return
		}
	}
}

// LimitedQueue is a bounded FIFO with backpressure, mirroring Async::LimitedQueue:
// a producer enqueuing a full queue suspends until a consumer makes room.
type LimitedQueue struct {
	limit      int
	items      []any
	deqWaiters []*Task
	enqWaiters []*Task
}

// NewLimitedQueue returns an empty queue holding at most limit items. A limit
// below one is clamped to one.
func NewLimitedQueue(limit int) *LimitedQueue {
	if limit < 1 {
		limit = 1
	}
	return &LimitedQueue{limit: limit}
}

// Enqueue appends an item, suspending the calling task while the queue is full,
// then wakes the longest-waiting consumer (Async::LimitedQueue#enqueue).
func (q *LimitedQueue) Enqueue(t *Task, v any) {
	for len(q.items) >= q.limit {
		q.enqWaiters = append(q.enqWaiters, t)
		t.suspend(func() { q.removeEnq(t) })
	}
	q.items = append(q.items, v)
	if len(q.deqWaiters) > 0 {
		w := q.deqWaiters[0]
		q.deqWaiters = q.deqWaiters[1:]
		w.wake()
	}
}

// Dequeue removes and returns the head item, suspending the calling task while
// the queue is empty, then wakes the longest-waiting producer
// (Async::LimitedQueue#dequeue).
func (q *LimitedQueue) Dequeue(t *Task) any {
	for len(q.items) == 0 {
		q.deqWaiters = append(q.deqWaiters, t)
		t.suspend(func() { q.removeDeq(t) })
	}
	v := q.items[0]
	q.items = q.items[1:]
	if len(q.enqWaiters) > 0 {
		w := q.enqWaiters[0]
		q.enqWaiters = q.enqWaiters[1:]
		w.wake()
	}
	return v
}

// Limit returns the maximum number of buffered items (Async::LimitedQueue#limit).
func (q *LimitedQueue) Limit() int { return q.limit }

// Size returns the number of buffered items.
func (q *LimitedQueue) Size() int { return len(q.items) }

// Empty reports whether the queue holds no items.
func (q *LimitedQueue) Empty() bool { return len(q.items) == 0 }

// LimitedQ reports whether the queue is at capacity (Async::LimitedQueue#limited?).
func (q *LimitedQueue) LimitedQ() bool { return len(q.items) >= q.limit }

func (q *LimitedQueue) removeEnq(t *Task) {
	for i, x := range q.enqWaiters {
		if x == t {
			q.enqWaiters = append(q.enqWaiters[:i], q.enqWaiters[i+1:]...)
			return
		}
	}
}

func (q *LimitedQueue) removeDeq(t *Task) {
	for i, x := range q.deqWaiters {
		if x == t {
			q.deqWaiters = append(q.deqWaiters[:i], q.deqWaiters[i+1:]...)
			return
		}
	}
}
