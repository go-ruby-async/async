// Package async is a pure-Go (no cgo), MRI-4.0.5-faithful model of the
// structured-concurrency core of Ruby's async gem (socketry/async).
//
// async is a fiber-based concurrency framework: an Async{} block runs a tree of
// cooperative tasks on a reactor, and blocking operations (waiting on a child,
// sleeping, acquiring a semaphore, reading a socket) suspend the running fiber
// back to the reactor instead of blocking a thread. This package reproduces both
// the *structured-concurrency* layer — the task tree with cancellation and
// failure propagation, plus the Async:: synchronization primitives — and the
// *IO reactor* on top of it, targeting a later rbgo binding where the host VM
// supplies real fibers.
//
// # The reactor and the Scheduler seam
//
// The point where a fiber suspends and is later resumed is expressed as an
// injectable Scheduler seam:
//
//	type Scheduler interface {
//		Defer(body func()) Fiber // spawn a fiber (a task) on the reactor
//		Yield() bool             // suspend the running fiber; false on teardown
//		Resume(Fiber)            // mark a suspended fiber runnable
//		Sleep(time.Duration)     // suspend the running fiber on the timer wheel
//		Run()                    // drive the loop to quiescence
//	}
//
// A Scheduler that also implements IOScheduler.AwaitIO can host the non-blocking
// IO reactor: instead of the gem's epoll/kqueue/io_uring event loop, an async IO
// operation runs the real (blocking) Go syscall on a goroutine — Go's runtime
// poller gives us genuine async IO natively — and parks just that one fiber
// until the syscall completes, keeping the loop alive meanwhile. The rbgo
// binding implements Scheduler on the host's real fibers and timer wheel. This
// package ships a deterministic, in-memory CoScheduler — a cooperative scheduler
// with a virtual clock plus this goroutine-backed IO reactor — so the whole
// model is exercised with no wall-clock sleeps for the pure-cooperative paths
// and no leaked goroutines, which is what keeps the test suite at 100% coverage.
//
// # Async IO
//
// Socket wraps a net.Conn and Listener wraps a net.Listener; their Read, Write,
// Accept and Connect suspend the calling fiber on the reactor rather than
// blocking a thread, and a Stop or with_timeout on the task cancels an in-flight
// operation (via a past IO deadline, a listener close, or a dial-context cancel).
// In-process net.Pipe and loopback TCP both work, so the reactor is exercised
// end to end without any external network.
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
// The package is CGO-free, depends only on the standard library, and is safe
// under the race detector.
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
