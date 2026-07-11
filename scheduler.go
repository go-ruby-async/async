package async

import "time"

// Fiber is an opaque handle to one cooperatively-scheduled unit of execution —
// one task's fiber. A Scheduler hands these out from Defer and takes them back
// in Resume. The concrete type is private to each Scheduler implementation.
type Fiber interface{ isFiber() }

// IOScheduler is the optional capability a Scheduler advertises when it can host
// asynchronous IO: the non-blocking IO reactor. A Task backs an Async::IO
// operation (socket read/write/accept/connect) by running the real, blocking
// syscall on a separate goroutine and parking its fiber via AwaitIO; the reactor
// keeps the loop alive until that goroutine reports completion and then resumes
// the fiber. This is how Go — which gives us real async IO natively through its
// runtime poller — stands in for the gem's epoll/kqueue/io_uring event loop.
//
// The bundled CoScheduler implements it. The rbgo binding implements it on the
// host VM's fibers plus the same goroutine-backed completion mechanism (or the
// host's own io_wait). A Scheduler that does not implement IOScheduler cannot
// host Async IO, and the IO wrappers return ErrNoIO.
type IOScheduler interface {
	Scheduler
	// AwaitIO runs op on a fresh goroutine and suspends the currently-running
	// fiber until op returns, then resumes it. It must be called only from a
	// running fiber. op performs the real (blocking) IO and stores its result in
	// variables the caller captured; op must not touch scheduler state. The
	// channel hand-off from the op goroutine back through the reactor to the
	// resumed fiber establishes the happens-before edge that makes reading those
	// captured variables race-free.
	AwaitIO(op func())
}

// Scheduler is the seam onto which the reactor is mapped. It is exactly the set
// of operations the structured-concurrency core needs from a fiber runtime:
// spawn a fiber (Defer), suspend the running fiber (Yield) and later resume it
// (Resume), suspend it on a timer (Sleep), and drive the loop (Run).
//
// The rbgo binding implements this on the host VM's real fibers plus a timer
// wheel (and, later, the non-blocking IO reactor). The bundled CoScheduler is a
// deterministic in-memory implementation used by the tests.
type Scheduler interface {
	// Defer spawns body as a new fiber scheduled to start on a later turn of the
	// loop, returning its handle.
	Defer(body func()) Fiber
	// Yield suspends the currently-running fiber, returning control to the loop.
	// It returns true when the fiber is later Resumed normally, and false when
	// the loop is tearing the fiber down (no remaining work can ever resume it),
	// in which case the caller must unwind.
	Yield() bool
	// Resume marks a suspended fiber runnable. It is a no-op if the fiber is not
	// currently suspended (already runnable, running, or finished).
	Resume(f Fiber)
	// Sleep suspends the currently-running fiber until at least d has elapsed on
	// the scheduler's clock. A non-positive d reschedules the fiber cooperatively
	// behind the other runnable fibers.
	Sleep(d time.Duration)
	// Run drives the loop until it is quiescent: no fiber is runnable, no timer
	// remains, and no IO is in flight. Fibers still blocked at quiescence are torn
	// down (their Yield returns false) so no goroutine is leaked. A Scheduler that
	// also implements IOScheduler keeps the loop alive while IO is outstanding.
	Run()
}

// fiberState tracks where a coFiber sits in the scheduler so Resume is
// idempotent and stale timer entries can be skipped.
type fiberState int

const (
	fReady   fiberState = iota // queued to run
	fRunning                   // currently executing
	fBlocked                   // suspended by Yield, awaiting an explicit Resume
	fTimer                     // suspended by Sleep, awaiting the timer
	fIO                        // suspended by AwaitIO, awaiting an IO completion
	fDone                      // body has returned
)

// coFiber is one cooperatively-scheduled goroutine. Exactly one coFiber runs at
// a time; control is handed between the scheduler loop and a fiber over the
// unbuffered resume/control channels, so every access to shared scheduler state
// is separated by a channel synchronisation and the race detector stays quiet.
type coFiber struct {
	resume    chan struct{}
	body      func()
	state     fiberState
	terminate bool // set by teardown so the fiber's Yield returns false
}

func (*coFiber) isFiber() {}

// coTimer is a pending Sleep: fib becomes runnable once the clock reaches wake.
type coTimer struct {
	wake time.Duration
	fib  *coFiber
}

// ioSignal is delivered by an IO goroutine over the scheduler's ioDone channel
// once its operation has returned, naming the fiber the reactor must resume.
type ioSignal struct{ fib *coFiber }

// CoScheduler is a deterministic, single-threaded cooperative scheduler with a
// virtual clock. It runs each fiber to its next suspension point in turn,
// advancing the clock only when no fiber is runnable, so timed behaviour is
// exercised without any real sleeping. It is the default Scheduler used by Run
// and the reference against which the rbgo binding's fiber scheduler is checked.
type CoScheduler struct {
	ready   []*coFiber
	timers  []coTimer
	blocked map[*coFiber]bool
	io      map[*coFiber]bool // fibers parked on an in-flight IO operation
	current *coFiber
	control chan struct{}
	ioDone  chan ioSignal // IO goroutines report completion here
	now     time.Duration
}

// NewScheduler returns a fresh deterministic cooperative scheduler.
func NewScheduler() *CoScheduler {
	return &CoScheduler{
		blocked: make(map[*coFiber]bool),
		io:      make(map[*coFiber]bool),
		control: make(chan struct{}),
		ioDone:  make(chan ioSignal),
	}
}

// Now returns the scheduler's current virtual time — the sum of the durations
// its Sleep calls have skipped past. It is zero until the first timer matures.
func (s *CoScheduler) Now() time.Duration { return s.now }

// Defer spawns body as a new fiber and queues it to start.
func (s *CoScheduler) Defer(body func()) Fiber {
	f := &coFiber{resume: make(chan struct{}), body: body, state: fReady}
	s.ready = append(s.ready, f)
	go func() {
		<-f.resume
		f.body()
		f.state = fDone
		s.control <- struct{}{}
	}()
	return f
}

// Yield suspends the running fiber until it is Resumed (returning true) or torn
// down (returning false).
func (s *CoScheduler) Yield() bool {
	self := s.current
	self.state = fBlocked
	s.blocked[self] = true
	s.control <- struct{}{}
	<-self.resume
	return !self.terminate
}

// Sleep suspends the running fiber on the timer wheel. A non-positive duration
// requeues it cooperatively behind the currently-runnable fibers.
func (s *CoScheduler) Sleep(d time.Duration) {
	self := s.current
	if d <= 0 {
		self.state = fReady
		s.ready = append(s.ready, self)
	} else {
		self.state = fTimer
		s.timers = append(s.timers, coTimer{wake: s.now + d, fib: self})
	}
	s.control <- struct{}{}
	<-self.resume
}

// AwaitIO runs op on a fresh goroutine and suspends the running fiber until op
// returns. The fiber is parked in the IO set, which keeps the loop from going
// quiescent (or tearing the fiber down) while the syscall is outstanding; the op
// goroutine reports back over ioDone, and the loop makes the fiber runnable
// again. This is the reactor's non-blocking-IO wait: a real, blocking Go IO call
// suspends one fiber (not the whole loop), exactly as the gem's io_wait suspends
// one fiber on the event loop.
func (s *CoScheduler) AwaitIO(op func()) {
	self := s.current
	go func() {
		op()
		s.ioDone <- ioSignal{self}
	}()
	self.state = fIO
	s.io[self] = true
	s.control <- struct{}{}
	<-self.resume
}

// Resume marks a suspended fiber runnable. Suspended fibers are either blocked
// (Yield) or timed (Sleep); either way the fiber is queued and any stale timer
// entry for it is skipped when the clock advances.
func (s *CoScheduler) Resume(fib Fiber) {
	f := fib.(*coFiber)
	switch f.state {
	case fBlocked:
		delete(s.blocked, f)
		f.state = fReady
		s.ready = append(s.ready, f)
	case fTimer:
		f.state = fReady
		s.ready = append(s.ready, f)
	}
}

// runOne hands control to f and blocks until f suspends or finishes.
func (s *CoScheduler) runOne(f *coFiber) {
	f.state = fRunning
	s.current = f
	f.resume <- struct{}{}
	<-s.control
	s.current = nil
}

// advanceTimers moves the clock forward to the earliest pending timer and makes
// every fiber due at that time runnable. It reports whether it did any work.
func (s *CoScheduler) advanceTimers() bool {
	next := time.Duration(-1)
	for _, t := range s.timers {
		if t.fib.state != fTimer {
			continue // stale: the fiber was resumed early
		}
		if next < 0 || t.wake < next {
			next = t.wake
		}
	}
	if next < 0 {
		s.timers = nil // only stale entries remained
		return false
	}
	s.now = next
	var rest []coTimer
	for _, t := range s.timers {
		if t.fib.state == fTimer && t.wake <= s.now {
			t.fib.state = fReady
			s.ready = append(s.ready, t.fib)
			continue
		}
		if t.fib.state == fTimer {
			rest = append(rest, t)
		}
	}
	s.timers = rest
	return true
}

// nextTimer returns the earliest pending (non-stale) timer wake and true, or
// false when only stale entries remain. It lets waitIO race a virtual timer
// against a real IO completion.
func (s *CoScheduler) nextTimer() (time.Duration, bool) {
	next := time.Duration(-1)
	for _, t := range s.timers {
		if t.fib.state != fTimer {
			continue // stale: the fiber was resumed early
		}
		if next < 0 || t.wake < next {
			next = t.wake
		}
	}
	if next < 0 {
		return 0, false
	}
	return next, true
}

// waitIO blocks the reactor until the next event while IO is outstanding: either
// an IO goroutine reports completion (resuming its fiber), or — if fibers are
// also parked on the virtual clock — a real timer, armed for the shortest
// remaining virtual delay, matures first and the clock advances. Mixing a real
// IO race with the virtual clock is what lets with_timeout wrap a real read: the
// timeout still fires even if the read would block forever.
func (s *CoScheduler) waitIO() {
	var timerC <-chan time.Time
	if next, ok := s.nextTimer(); ok {
		// next is strictly in the future: matured timers are readied before the
		// loop ever parks on IO, so the earliest pending wake is beyond now.
		tm := time.NewTimer(next - s.now)
		defer tm.Stop()
		timerC = tm.C
	}
	select {
	case sig := <-s.ioDone:
		delete(s.io, sig.fib)
		sig.fib.state = fReady
		s.ready = append(s.ready, sig.fib)
	case <-timerC:
		s.advanceTimers()
	}
}

// teardown makes every still-blocked fiber runnable with its terminate flag set,
// so that when the loop next runs it its Yield returns false and it unwinds. It
// is invoked only when the loop would otherwise be stuck.
func (s *CoScheduler) teardown() {
	for f := range s.blocked {
		delete(s.blocked, f)
		f.terminate = true
		f.state = fReady
		s.ready = append(s.ready, f)
	}
}

// Run drives the loop to quiescence.
func (s *CoScheduler) Run() {
	for {
		if len(s.ready) > 0 {
			f := s.ready[0]
			s.ready = s.ready[1:]
			s.runOne(f)
			continue
		}
		if len(s.io) > 0 {
			s.waitIO()
			continue
		}
		if s.advanceTimers() {
			continue
		}
		if len(s.blocked) > 0 {
			s.teardown()
			continue
		}
		return
	}
}
