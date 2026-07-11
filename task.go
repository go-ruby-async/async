package async

import (
	"fmt"
	"time"
)

// Body is the work a Task runs: it receives its own Task (so it can spawn
// children, sleep, or wait) and returns a result value or a failure. It is the
// Go shape of the Ruby block passed to Async{} / task.async{}; the rbgo binding
// wraps the host VM's block as a Body.
type Body func(t *Task) (any, error)

// cancelSignal is the panic value used to unwind a task at a suspension point
// when it is cancelled, mirroring an Async::Stop / Async::TimeoutError raised
// inside the fiber. It is always recovered by Task.run.
type cancelSignal struct{ err error }

// Task is one node of the structured-concurrency tree: a unit of work running on
// its own fiber, with a lifecycle state, a result or failure, a parent, and a
// set of children. It mirrors Async::Task.
type Task struct {
	sched    Scheduler
	fiber    Fiber
	parent   *Task
	children map[*Task]bool

	body       Body
	state      State
	result     any
	err        error
	cancelErr  error  // non-nil once the task is asked to stop/time out
	suspended  bool   // true while parked at a blocking point
	onCancelIO func() // set while parked on IO: unblocks the syscall so it returns
	waiters    []*Task
}

// Run starts a reactor and runs body as its root task, returning the root's
// result or failure once the reactor is quiescent. It is the equivalent of
// Ruby's Async{ |task| ... }. The default scheduler is a deterministic
// cooperative one; use RunOn to supply a host fiber scheduler.
func Run(body Body) (any, error) {
	return RunOn(NewScheduler(), body)
}

// RunOn runs body as the root task on the given Scheduler and drives it to
// quiescence. A binding passes its own fiber-backed Scheduler here.
func RunOn(s Scheduler, body Body) (any, error) {
	root := newTask(s, nil, body)
	root.schedule()
	s.Run()
	if root.state == Failed {
		return nil, root.err
	}
	if root.state == Stopped {
		return nil, ErrStop
	}
	return root.result, nil
}

func newTask(s Scheduler, parent *Task, body Body) *Task {
	return &Task{
		sched:    s,
		parent:   parent,
		children: make(map[*Task]bool),
		body:     body,
		state:    Initialized,
	}
}

// schedule defers the task's body onto the scheduler as a new fiber.
func (t *Task) schedule() { t.fiber = t.sched.Defer(t.run) }

// Async spawns a child task running body and schedules it to run. It is the
// equivalent of task.async{ |child| ... }: the child joins this task's subtree,
// so stopping or failing this task stops the child.
func (t *Task) Async(body Body) *Task {
	c := newTask(t.sched, t, body)
	t.children[c] = true
	c.schedule()
	return c
}

// run is the fiber body: it executes the task's Body, capturing the result,
// failure, or cancellation, then finalises the task.
func (t *Task) run() {
	if t.cancelErr != nil {
		// Stopped before the body ever started.
		t.state = Stopped
		t.err = t.cancelErr
		t.finish()
		return
	}
	t.state = Running
	defer func() {
		if r := recover(); r != nil {
			if cs, ok := r.(cancelSignal); ok {
				if cs.err == ErrStop {
					t.state, t.err = Stopped, ErrStop
				} else {
					t.state, t.err = Failed, cs.err
				}
			} else {
				t.state, t.err = Failed, asError(r)
			}
		}
		t.finish()
	}()
	res, err := t.body(t)
	if err != nil {
		t.state, t.err = Failed, err
	} else {
		t.state, t.result = Complete, res
	}
}

// finish stops the task's remaining children if it did not complete normally,
// wakes everyone waiting on it, and detaches it from its parent.
func (t *Task) finish() {
	if t.state == Failed || t.state == Stopped {
		for _, c := range t.snapshotChildren() {
			c.Stop()
		}
	}
	for _, w := range t.waiters {
		w.wake()
	}
	t.waiters = nil
	if t.parent != nil {
		delete(t.parent.children, t)
	}
}

// suspend parks the task at a blocking point until it is woken or torn down. If
// the task is cancelled while parked, onCancel (used by primitives to drop the
// task from their wait queues) runs and the task unwinds via panic.
func (t *Task) suspend(onCancel func()) {
	t.suspended = true
	alive := t.sched.Yield()
	t.suspended = false
	if !alive {
		t.cancelErr = ErrStop
	}
	if t.cancelErr != nil {
		if onCancel != nil {
			onCancel()
		}
		panic(cancelSignal{t.cancelErr})
	}
}

// wake makes a suspended task runnable again.
func (t *Task) wake() { t.sched.Resume(t.fiber) }

// cancel records a cancellation reason and provokes the task to unwind at its
// suspension point. It is only ever called on a task parked at a blocking point
// (from Stop, or from a WithTimeout timer). A task parked on IO cannot be woken
// directly — its fiber will only be resumed once the outstanding syscall
// returns — so cancel invokes the IO-cancel hook (which sets a past deadline or
// closes the resource) to make that syscall return promptly; the reactor then
// resumes the fiber, which observes cancelErr and unwinds. Otherwise cancel just
// wakes the suspended fiber.
func (t *Task) cancel(err error) {
	t.cancelErr = err
	if t.onCancelIO != nil {
		t.onCancelIO()
		return
	}
	t.wake()
}

// awaitIO parks the task on an in-flight asynchronous IO operation: op performs
// the real (blocking) syscall on a reactor-spawned goroutine while this fiber
// suspends, and cancel unblocks that syscall if the task is stopped or times out
// while parked. It returns ErrNoIO when the scheduler cannot host IO; on
// cancellation it unwinds the fiber via the usual cancelSignal, exactly like any
// other suspension point.
func (t *Task) awaitIO(op func(), cancel func()) error {
	ios, ok := t.sched.(IOScheduler)
	if !ok {
		return ErrNoIO
	}
	t.suspended = true
	t.onCancelIO = cancel
	ios.AwaitIO(op)
	t.suspended = false
	t.onCancelIO = nil
	if t.cancelErr != nil {
		panic(cancelSignal{t.cancelErr})
	}
	return nil
}

// Wait joins the task, blocking the calling task until it finishes, then
// returns its result or its failure — ErrStop if it was stopped. It mirrors
// Async::Task#wait. The caller is the currently-running task (the binding passes
// Async::Task.current).
func (t *Task) Wait(caller *Task) (any, error) {
	if !t.isDone() {
		t.waiters = append(t.waiters, caller)
		caller.suspend(func() { t.removeWaiter(caller) })
	}
	if t.state == Failed {
		return nil, t.err
	}
	if t.state == Stopped {
		return nil, ErrStop
	}
	return t.result, nil
}

// Stop cancels the task and, structurally, its children. A parked task unwinds
// (Async::Stop) at its suspension point; a task that has not started never runs
// its body; a task stopping itself unwinds immediately. It mirrors
// Async::Task#stop.
func (t *Task) Stop() {
	if t.isDone() {
		return
	}
	for _, c := range t.snapshotChildren() {
		c.Stop()
	}
	switch t.state {
	case Initialized:
		t.cancelErr = ErrStop
	case Running:
		if t.suspended {
			t.cancel(ErrStop)
		} else {
			// Stopping the currently-running task: raise now.
			t.cancelErr = ErrStop
			panic(cancelSignal{ErrStop})
		}
	}
}

// Sleep suspends the task for at least d on the scheduler's clock, and honours a
// concurrent Stop/timeout by unwinding when it wakes. It mirrors task.sleep.
func (t *Task) Sleep(d time.Duration) {
	t.suspended = true
	t.sched.Sleep(d)
	t.suspended = false
	if t.cancelErr != nil {
		panic(cancelSignal{t.cancelErr})
	}
}

// Yield reschedules the task cooperatively behind the other runnable tasks,
// honouring a concurrent cancellation. It mirrors task.yield / Fiber.yield to
// the reactor.
func (t *Task) Yield() { t.Sleep(0) }

// WithTimeout runs fn under a timeout budget d measured on the scheduler's
// clock. If fn does not finish within d, ErrTimeout is raised inside it (at its
// next suspension point) and returned; otherwise fn's own result is returned. It
// mirrors task.with_timeout(d){ ... } raising Async::TimeoutError.
func (t *Task) WithTimeout(d time.Duration, fn Body) (any, error) {
	work := t.Async(fn)
	timer := t.Async(func(tt *Task) (any, error) {
		tt.Sleep(d)
		work.cancel(ErrTimeout)
		return nil, nil
	})
	res, err := work.Wait(t)
	timer.Stop()
	_, _ = timer.Wait(t)
	return res, err
}

// Result returns the task's result value, or nil if it has not completed
// successfully. It mirrors Async::Task#result without re-raising.
func (t *Task) Result() any {
	if t.state == Complete {
		return t.result
	}
	return nil
}

// Err returns the task's failure (or ErrStop for a stopped task), or nil.
func (t *Task) Err() error { return t.err }

// State returns the task's lifecycle state.
func (t *Task) State() State { return t.state }

// Parent returns the task's parent, or nil for a root task.
func (t *Task) Parent() *Task { return t.parent }

// Children returns a snapshot of the task's live children.
func (t *Task) Children() []*Task { return t.snapshotChildren() }

// Scheduler returns the scheduler the task runs on.
func (t *Task) Scheduler() Scheduler { return t.sched }

// RunningQ reports whether the task is running (Async::Task#running?).
func (t *Task) RunningQ() bool { return t.state == Running }

// CompleteQ reports whether the task completed successfully (#completed?).
func (t *Task) CompleteQ() bool { return t.state == Complete }

// FailedQ reports whether the task failed (#failed?).
func (t *Task) FailedQ() bool { return t.state == Failed }

// StoppedQ reports whether the task was stopped (#stopped?).
func (t *Task) StoppedQ() bool { return t.state == Stopped }

func (t *Task) isDone() bool {
	switch t.state {
	case Complete, Failed, Stopped:
		return true
	}
	return false
}

func (t *Task) snapshotChildren() []*Task {
	out := make([]*Task, 0, len(t.children))
	for c := range t.children {
		out = append(out, c)
	}
	return out
}

func (t *Task) removeWaiter(w *Task) {
	for i, x := range t.waiters {
		if x == w {
			t.waiters = append(t.waiters[:i], t.waiters[i+1:]...)
			return
		}
	}
}

// asError coerces a recovered panic value to an error.
func asError(r any) error {
	if e, ok := r.(error); ok {
		return e
	}
	return fmt.Errorf("async: task panicked: %v", r)
}
