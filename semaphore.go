package async

// Semaphore is a counting semaphore that blocks acquirers once its limit is
// reached, mirroring Async::Semaphore. Waiting acquirers suspend their fiber
// (rather than a thread) and are resumed in FIFO order as permits are released.
type Semaphore struct {
	limit int
	count int
	queue []*Task
}

// NewSemaphore returns a semaphore permitting limit concurrent holders. A limit
// below one is clamped to one.
func NewSemaphore(limit int) *Semaphore {
	if limit < 1 {
		limit = 1
	}
	return &Semaphore{limit: limit}
}

// Acquire takes one permit, suspending the calling task while the semaphore is
// at its limit (Async::Semaphore#acquire).
func (s *Semaphore) Acquire(t *Task) {
	for s.count >= s.limit {
		s.queue = append(s.queue, t)
		t.suspend(func() { s.removeWaiter(t) })
	}
	s.count++
}

// Release returns one permit and wakes the next waiting task, if any
// (Async::Semaphore#release).
func (s *Semaphore) Release() {
	if s.count > 0 {
		s.count--
	}
	if len(s.queue) > 0 {
		w := s.queue[0]
		s.queue = s.queue[1:]
		w.wake()
	}
}

// AcquireDo acquires a permit, runs fn, and releases the permit even if fn
// panics, mirroring the block form Async::Semaphore#acquire{ ... }.
func (s *Semaphore) AcquireDo(t *Task, fn func() (any, error)) (any, error) {
	s.Acquire(t)
	defer s.Release()
	return fn()
}

// Count returns the number of permits currently held (Async::Semaphore#count).
func (s *Semaphore) Count() int { return s.count }

// Limit returns the maximum number of concurrent holders
// (Async::Semaphore#limit).
func (s *Semaphore) Limit() int { return s.limit }

// SetLimit adjusts the permit limit (Async::Semaphore#limit=). Raising it wakes
// as many waiting tasks as the new headroom allows. A limit below one is clamped
// to one.
func (s *Semaphore) SetLimit(limit int) {
	if limit < 1 {
		limit = 1
	}
	s.limit = limit
	for s.count < s.limit && len(s.queue) > 0 {
		w := s.queue[0]
		s.queue = s.queue[1:]
		w.wake()
	}
}

// Blocking reports whether a further Acquire would block (Async::Semaphore#blocking?).
func (s *Semaphore) Blocking() bool { return s.count >= s.limit }

// Waiting returns the number of tasks blocked in Acquire.
func (s *Semaphore) Waiting() int { return len(s.queue) }

func (s *Semaphore) removeWaiter(t *Task) {
	for i, x := range s.queue {
		if x == t {
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			return
		}
	}
}
