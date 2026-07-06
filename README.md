<p align="center"><img src="https://raw.githubusercontent.com/go-ruby-async/brand/main/social/go-ruby-async-async.png" alt="go-ruby-async/async" width="720"></p>

# async — go-ruby-async

[![Docs](https://img.shields.io/badge/docs-mkdocs--material-DC2626)](https://go-ruby-async.github.io/docs/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](https://go.dev/dl/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-1a7f37)](#tests--coverage)

**A pure-Go (no cgo) reimplementation of the structured-concurrency core of
Ruby's [`async`](https://github.com/socketry/async) gem** — the fiber-based task
tree with cancellation and failure propagation, plus the `Async::` synchronization
primitives — modelling the gem's observable behaviour and vocabulary **without any
Ruby runtime**.

It is the `async` backend for
[go-embedded-ruby](https://github.com/go-embedded-ruby/ruby) (a later
`require "async"` binding), but is a **standalone, reusable** Go module — a
sibling of [go-ruby-concurrent-ruby](https://github.com/go-ruby-concurrent-ruby/concurrent-ruby)
and [go-ruby-set](https://github.com/go-ruby-set/set).

## Scope: the structured-concurrency core, with the IO reactor deferred

async's deepest value is its **non-blocking IO reactor** — an epoll/kqueue/io_uring
event loop that suspends a fiber when its socket would block and resumes it when
the socket is ready. That IO reactor is **deliberately not ported here.** What
this package ships is the *structured-concurrency layer that sits on top of it*:
the task tree, cancellation, failure propagation, and the synchronization
primitives. The point where a fiber suspends and is later resumed is factored out
into an injectable **Scheduler seam**, so the rbgo binding supplies the host VM's
real fibers (and, eventually, the IO reactor), while the tests drive a
deterministic in-memory scheduler.

| Modelled (this package) | Deferred (the Scheduler binding) |
| --- | --- |
| `Task` tree: `Async{}` / `task.async{}`, states, `wait`/`stop`/`result` | Real fiber suspend/resume (rbgo fibers) |
| Structured cancellation (stopping a parent stops its children) | The non-blocking **IO reactor** (epoll/kqueue/io_uring) |
| Failure propagation (`wait` re-raises; a failed task stops its children) | The wall-clock **timer wheel** (here: a virtual clock) |
| `Barrier`, `Semaphore`, `Condition`, `Notification`, `Queue`, `LimitedQueue`, `Waiter` | `io_wait` / `io_read` / `io_write` / address resolution |
| `Async::Stop`, `Async::TimeoutError` | |

## The Scheduler seam

Everywhere the gem's reactor would suspend and resume a fiber, this package calls
through one small interface, so a binding can map it onto the host runtime:

```go
type Scheduler interface {
	Defer(body func()) Fiber // spawn a fiber (a task) on the reactor
	Yield() bool             // suspend the running fiber; false on teardown
	Resume(Fiber)            // mark a suspended fiber runnable
	Sleep(time.Duration)     // suspend the running fiber on the timer wheel
	Run()                    // drive the loop to quiescence
}
```

The rbgo binding implements `Scheduler` on `Fiber.yield` / `Fiber#resume` plus a
timer wheel. This package bundles **`CoScheduler`**, a deterministic cooperative
scheduler with a **virtual clock**: it runs each task to its next suspension point
in turn, advancing time only when nothing is runnable, so timed behaviour is
exercised with **zero wall-clock sleeps and no leaked goroutines** — which is what
keeps the suite at 100% coverage. Tasks still blocked when the loop goes quiescent
are torn down (their `Yield` returns `false`), so a well-formed program never
strands a fiber.

Every blocking method takes the calling `*Task` (the binding passes
`Async::Task.current`) so it suspends the right fiber and observes cancellation.

## Install

```sh
go get github.com/go-ruby-async/async
```

## Usage

```go
package main

import (
	"fmt"
	"time"

	"github.com/go-ruby-async/async"
)

func main() {
	// Async{ |task| ... } — a root task on a fresh reactor.
	total, _ := async.Run(func(task *async.Task) (any, error) {
		sem := async.NewSemaphore(2) // at most 2 in flight
		barrier := async.NewBarrier()
		sum := 0

		for i := 1; i <= 5; i++ {
			i := i
			// barrier.async { ... } — a child task in the subtree.
			barrier.Async(task, func(child *async.Task) (any, error) {
				return sem.AcquireDo(child, func() (any, error) {
					child.Sleep(10 * time.Millisecond) // non-blocking
					sum += i
					return nil, nil
				})
			})
		}

		if err := barrier.Wait(task); err != nil { // join all children
			return nil, err
		}
		return sum, nil
	})

	fmt.Println(total) // 15
}
```

## API

```go
// Reactor entry points
func Run(body Body) (any, error)                 // Async{ |task| ... }
func RunOn(s Scheduler, body Body) (any, error)  // run on a host scheduler
type Body func(t *Task) (any, error)             // the Ruby block

// Task tree (Async::Task)
func (t *Task) Async(body Body) *Task            // task.async{ ... }
func (t *Task) Wait(caller *Task) (any, error)   // wait (re-raises failure)
func (t *Task) Stop()                            // stop (cascades to children)
func (t *Task) Sleep(d time.Duration)            // task.sleep
func (t *Task) Yield()                           // cooperative reschedule
func (t *Task) WithTimeout(d time.Duration, fn Body) (any, error) // with_timeout
func (t *Task) Result() any                      // result (no re-raise)
func (t *Task) Err() error
func (t *Task) State() State                     // Initialized/Running/Complete/Stopped/Failed
func (t *Task) RunningQ() bool                   // running?
func (t *Task) CompleteQ() bool                  // completed?
func (t *Task) FailedQ() bool                    // failed?
func (t *Task) StoppedQ() bool                   // stopped?
func (t *Task) Parent() *Task
func (t *Task) Children() []*Task
func (t *Task) Scheduler() Scheduler

// Primitives (the Async:: namespace)
func NewBarrier() *Barrier                        // Async::Barrier
func (b *Barrier) Async(parent *Task, body Body) *Task
func (b *Barrier) Wait(caller *Task) error        // wait for all
func (b *Barrier) Stop()

func NewSemaphore(limit int) *Semaphore           // Async::Semaphore
func (s *Semaphore) Acquire(t *Task)
func (s *Semaphore) Release()
func (s *Semaphore) AcquireDo(t *Task, fn func() (any, error)) (any, error)
func (s *Semaphore) Count() int
func (s *Semaphore) Limit() int
func (s *Semaphore) SetLimit(limit int)
func (s *Semaphore) Blocking() bool

func NewCondition() *Condition                    // Async::Condition
func (c *Condition) Wait(t *Task) any
func (c *Condition) Signal(value any)

func NewNotification() *Notification              // Async::Notification
func (n *Notification) Wait(t *Task)
func (n *Notification) Signal()

func NewQueue() *Queue                            // Async::Queue
func (q *Queue) Enqueue(v any)
func (q *Queue) Dequeue(t *Task) any

func NewLimitedQueue(limit int) *LimitedQueue     // Async::LimitedQueue (backpressure)
func (q *LimitedQueue) Enqueue(t *Task, v any)
func (q *LimitedQueue) Dequeue(t *Task) any

func NewWaiter(parent *Task) *Waiter              // Async::Waiter
func (w *Waiter) Async(body Body) *Task
func (w *Waiter) Wait(caller *Task, count int) ([]any, error)

// Errors (raised into the host VM by the binding)
var ErrStop    error // Async::Stop
var ErrTimeout error // Async::TimeoutError
```

`Wait` returns a stopped task's result as `ErrStop` and a failed task's as its
error, mirroring `Async::Task#wait` re-raising. `Stop` raises the cancellation at
the task's next suspension point (as async raises `Async::Stop` at the next fiber
yield) and cascades to the task's children; a task that fails or is stopped stops
its still-running children. The primitives suspend the calling fiber rather than a
thread, exactly as the gem does.

## Fidelity basis

Fidelity here is **behavioural**, pinned by the deterministic cooperative
scheduler: unlike a stdlib library, async is a *gem* whose semantics only exist
inside a running reactor, so there is no live-gem differential oracle. The task
states, the parent→children cancellation tree, failure propagation, and each
primitive are checked against the documented behaviour of async 2.x on MRI 4.0.5.
The IO reactor is out of scope by design (see the table above) and is supplied by
the Scheduler binding.

## Tests & coverage

The deterministic, virtual-clock scheduler drives every test, so the whole model —
all task states, cancellation and failure propagation, teardown of stranded
tasks, and each primitive — is exercised with **no `time.Sleep` for correctness
and no leaked goroutines**, holding coverage at **100%**.

```sh
COVERPKG=$(go list ./... | paste -sd, -)
go test -race -coverpkg="$COVERPKG" -coverprofile=cover.out ./...
go tool cover -func=cover.out | tail -1   # 100.0%
```

CGO-free, dependency-free, `gofmt` + `go vet` clean, `-race` clean, and green
across the six 64-bit Go targets (amd64, arm64, riscv64, loong64, ppc64le, s390x)
and three OSes (Linux, macOS, Windows).

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright the go-ruby-async/async authors.
