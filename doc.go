// Package async is a pure-Go (no cgo), MRI-4.0.5-faithful model of the
// structured-concurrency core of Ruby's async gem (socketry/async).
//
// async is a fiber-based concurrency framework: an Async{} block runs a tree of
// cooperative tasks on a reactor, and blocking operations (waiting on a child,
// sleeping, acquiring a semaphore) suspend the running fiber back to the reactor
// instead of blocking a thread. This package reproduces that *structured-
// concurrency* layer — the task tree with cancellation and failure propagation,
// plus the Async:: synchronization primitives — targeting a later rbgo binding
// where the host VM supplies real fibers.
//
// # What is modelled, and what is deferred
//
// async's deepest value is its non-blocking IO reactor (an epoll/kqueue/io_uring
// event loop that resumes fibers when their sockets become ready). That IO
// reactor is deliberately NOT ported here. Instead, the point where a fiber
// suspends and is later resumed is expressed as an injectable Scheduler seam:
//
//	type Scheduler interface {
//		Defer(body func()) Fiber // spawn a fiber (a task) on the reactor
//		Yield() bool             // suspend the running fiber; false on teardown
//		Resume(Fiber)            // mark a suspended fiber runnable
//		Sleep(time.Duration)     // suspend the running fiber on the timer wheel
//		Run()                    // drive the loop to quiescence
//	}
//
// The rbgo binding implements Scheduler on top of the host's real fibers and a
// timer wheel (and, eventually, the IO reactor). This package ships a
// deterministic, in-memory CoScheduler — a cooperative scheduler with a virtual
// clock — so the whole model is exercised with zero wall-clock sleeps and no
// leaked goroutines, which is what keeps the test suite at 100% coverage.
//
// # Task tree
//
// Run (Async{}) starts a root Task; Task.Async (task.async{}) spawns a child.
// Every Task carries its result/failure and a lifecycle state
// (Initialized/Running/Complete/Stopped/Failed). Task.Wait joins a task,
// returning its result or its failure (ErrStop for a stopped task); Task.Stop
// cancels a task and, structurally, its children — cancellation is raised at the
// task's next suspension point, exactly as async raises Async::Stop at the next
// fiber yield. A task that fails or is stopped stops its still-running children.
//
// # Primitives (the Async:: namespace)
//
// Barrier, Semaphore, Condition, Notification, Queue, LimitedQueue and Waiter
// mirror the gem's synchronization objects. Each blocking method takes the
// calling *Task (the binding passes Async::Task.current) so it can suspend the
// right fiber and observe cancellation.
//
// The package is CGO-free, dependency-free, and safe under the race detector.
package async

import "errors"

// State is the lifecycle state of a Task, mirroring async's task status symbols
// :initialized, :running, :completed, :stopped, and :failed.
type State string

const (
	// Initialized is the state of a task that has been created but whose body
	// has not started running yet.
	Initialized State = "initialized"
	// Running is the state of a task whose body is executing or suspended at a
	// blocking point.
	Running State = "running"
	// Complete is the state of a task whose body returned normally
	// (async :completed).
	Complete State = "complete"
	// Stopped is the state of a task that was cancelled — its Async::Stop
	// equivalent was raised at a suspension point and unwound the body.
	Stopped State = "stopped"
	// Failed is the state of a task whose body returned an error or panicked
	// (other than a cancellation).
	Failed State = "failed"
)

// Package-level sentinel errors mirroring async's exception classes. A binding
// maps each to the corresponding Ruby exception when raising into the host VM.
var (
	// ErrStop mirrors Async::Stop: the exception raised inside a task's fiber to
	// cancel it. Waiting on a stopped task returns ErrStop.
	ErrStop = errors.New("async: stop")

	// ErrTimeout mirrors Async::TimeoutError, raised inside a task when a
	// Task.WithTimeout budget elapses before its block completes.
	ErrTimeout = errors.New("async: timeout")
)
