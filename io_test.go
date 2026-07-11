package async

import (
	"errors"
	"net"
	"testing"
	"time"
)

// readN reads exactly len(buf) bytes through the reactor, looping over short
// reads (a stream socket may satisfy a read in several chunks).
func readN(ct *Task, s *Socket, buf []byte) error {
	for off := 0; off < len(buf); {
		n, err := s.Read(ct, buf[off:])
		if err != nil {
			return err
		}
		off += n
	}
	return nil
}

// TestSocketReadWrite drives one task writing while another reads over an
// in-process net.Pipe: both park on the reactor and complete without blocking a
// thread. Mirrors Async::IO read/write on a duplex stream.
func TestSocketReadWrite(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	var got []byte
	_, err := Run(func(task *Task) (any, error) {
		a, b := Wrap(c1), Wrap(c2)
		if a.Conn() != c1 {
			t.Errorf("Conn mismatch")
		}
		writer := task.Async(func(ct *Task) (any, error) {
			return a.Write(ct, []byte("hello"))
		})
		reader := task.Async(func(ct *Task) (any, error) {
			buf := make([]byte, 5)
			if err := readN(ct, b, buf); err != nil {
				return nil, err
			}
			got = buf
			return nil, nil
		})
		if _, err := writer.Wait(task); err != nil {
			return nil, err
		}
		return nil, mustWait(reader, task)
	})
	if err != nil {
		t.Fatalf("run err = %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q", got)
	}
}

func mustWait(task, caller *Task) error {
	_, err := task.Wait(caller)
	return err
}

// TestListenerAcceptConnect exercises the full accept/connect/read/write path
// over a loopback TCP listener, entirely in-process (127.0.0.1, OS-assigned
// port). Mirrors Async::IO::Endpoint accept + connect.
func TestListenerAcceptConnect(t *testing.T) {
	ln, err := Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := ln.Addr().String()
	var serverGot []byte
	_, err = Run(func(task *Task) (any, error) {
		server := task.Async(func(ct *Task) (any, error) {
			conn, err := ln.Accept(ct)
			if err != nil {
				return nil, err
			}
			defer conn.Close()
			buf := make([]byte, 4)
			if err := readN(ct, conn, buf); err != nil {
				return nil, err
			}
			serverGot = buf
			return nil, nil
		})
		client := task.Async(func(ct *Task) (any, error) {
			conn, err := Connect(ct, "tcp", addr)
			if err != nil {
				return nil, err
			}
			defer conn.Close()
			_, err = conn.Write(ct, []byte("ping"))
			return nil, err
		})
		if err := mustWait(server, task); err != nil {
			return nil, err
		}
		return nil, mustWait(client, task)
	})
	if err != nil {
		t.Fatalf("run err = %v", err)
	}
	if string(serverGot) != "ping" {
		t.Fatalf("server got %q", serverGot)
	}
}

// TestSocketReadStopped stops a task parked on a read: the reactor unblocks the
// syscall (past read deadline) and the task unwinds with Async::Stop.
func TestSocketReadStopped(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	Run(func(task *Task) (any, error) {
		reader := task.Async(func(ct *Task) (any, error) {
			buf := make([]byte, 1)
			_, err := Wrap(c2).Read(ct, buf) // no data ever arrives
			return nil, err
		})
		task.Yield() // let the reader park on the read
		reader.Stop()
		_, err := reader.Wait(task)
		if !errors.Is(err, ErrStop) {
			t.Errorf("wait err = %v", err)
		}
		if !reader.StoppedQ() {
			t.Errorf("state = %v", reader.State())
		}
		return nil, nil
	})
}

// TestSocketWriteStopped stops a task parked on a write (net.Pipe blocks a write
// until the peer reads): the reactor unblocks it via a past write deadline and
// the task unwinds with Async::Stop.
func TestSocketWriteStopped(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	Run(func(task *Task) (any, error) {
		writer := task.Async(func(ct *Task) (any, error) {
			// No peer ever reads, so the write blocks until cancelled.
			_, err := Wrap(c1).Write(ct, []byte("stuck"))
			return nil, err
		})
		task.Yield()
		writer.Stop()
		_, err := writer.Wait(task)
		if !errors.Is(err, ErrStop) {
			t.Errorf("wait err = %v", err)
		}
		if !writer.StoppedQ() {
			t.Errorf("state = %v", writer.State())
		}
		return nil, nil
	})
}

// TestSocketReadTimeout wraps a blocking read in with_timeout: the virtual-clock
// timer is raced against the real read in the reactor, fires first, and raises
// Async::TimeoutError into the read. This also exercises the reactor's
// timer-vs-IO race path.
func TestSocketReadTimeout(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	_, err := Run(func(task *Task) (any, error) {
		return task.WithTimeout(20*time.Millisecond, func(ct *Task) (any, error) {
			buf := make([]byte, 1)
			return Wrap(c2).Read(ct, buf) // blocks forever without the timeout
		})
	})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("err = %v", err)
	}
}

// TestReactorSkipsStaleTimer forces the reactor's waitIO path to iterate a stale
// timer entry (left by a sleeping task that was stopped early) while real IO is
// outstanding, covering the stale-skip branch of nextTimer.
func TestReactorSkipsStaleTimer(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	Run(func(task *Task) (any, error) {
		// Phase 1: a sleeping task stopped early leaves a stale timer entry.
		victim := task.Async(func(ct *Task) (any, error) {
			ct.Sleep(time.Hour)
			return nil, nil
		})
		task.Yield()  // victim parks on the timer
		victim.Stop() // resumes it early; its timer entry goes stale
		_, _ = victim.Wait(task)

		// Phase 2: a real read is outstanding while a live timer is pending, so
		// the reactor's waitIO iterates the lingering stale entry.
		reader := task.Async(func(ct *Task) (any, error) {
			buf := make([]byte, 1)
			_, err := Wrap(c2).Read(ct, buf)
			return nil, err
		})
		task.Sleep(10 * time.Millisecond) // live timer; matures in real time
		reader.Stop()
		_, _ = reader.Wait(task)
		return nil, nil
	})
}

// noIOSched is a Scheduler that does not implement IOScheduler, used to check the
// ErrNoIO fallback of every IO wrapper.
type noIOSched struct{}

func (noIOSched) Defer(func()) Fiber  { return nil }
func (noIOSched) Yield() bool         { return true }
func (noIOSched) Resume(Fiber)        {}
func (noIOSched) Sleep(time.Duration) {}
func (noIOSched) Run()                {}

func TestIOWithoutIOScheduler(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	ln, err := Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	task := &Task{sched: noIOSched{}}
	if _, err := Wrap(c1).Read(task, make([]byte, 1)); !errors.Is(err, ErrNoIO) {
		t.Errorf("Read err = %v", err)
	}
	if _, err := Wrap(c1).Write(task, []byte("x")); !errors.Is(err, ErrNoIO) {
		t.Errorf("Write err = %v", err)
	}
	if _, err := ln.Accept(task); !errors.Is(err, ErrNoIO) {
		t.Errorf("Accept err = %v", err)
	}
	if _, err := Connect(task, "tcp", ln.Addr().String()); !errors.Is(err, ErrNoIO) {
		t.Errorf("Connect err = %v", err)
	}
}

// blockListener is a net.Listener without SetDeadline: its Accept blocks until
// Close, so cancelling an Accept on it must fall back to closing the listener.
type blockListener struct{ ch chan struct{} }

func (b *blockListener) Accept() (net.Conn, error) {
	<-b.ch
	return nil, errors.New("blockListener: closed")
}
func (b *blockListener) Close() error   { close(b.ch); return nil }
func (b *blockListener) Addr() net.Addr { return dummyAddr{} }

type dummyAddr struct{}

func (dummyAddr) Network() string { return "block" }
func (dummyAddr) String() string  { return "block" }

// TestAcceptCancelNoDeadline cancels an Accept on a listener that lacks
// SetDeadline, covering cancelAccept's Close fallback.
func TestAcceptCancelNoDeadline(t *testing.T) {
	l := WrapListener(&blockListener{ch: make(chan struct{})})
	if l.Addr().Network() != "block" {
		t.Errorf("addr = %v", l.Addr())
	}
	Run(func(task *Task) (any, error) {
		acc := task.Async(func(ct *Task) (any, error) {
			_, err := l.Accept(ct)
			return nil, err
		})
		task.Yield()
		acc.Stop()
		_, err := acc.Wait(task)
		if !errors.Is(err, ErrStop) {
			t.Errorf("err = %v", err)
		}
		return nil, nil
	})
}

// TestAcceptCancelWithDeadline cancels an Accept on a real TCP listener, covering
// cancelAccept's SetDeadline path.
func TestAcceptCancelWithDeadline(t *testing.T) {
	ln, err := Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	Run(func(task *Task) (any, error) {
		acc := task.Async(func(ct *Task) (any, error) {
			_, err := ln.Accept(ct) // no client ever connects
			return nil, err
		})
		task.Yield()
		acc.Stop()
		_, err := acc.Wait(task)
		if !errors.Is(err, ErrStop) {
			t.Errorf("err = %v", err)
		}
		return nil, nil
	})
}

// TestAcceptError closes a listener out from under a parked Accept (not via task
// cancellation), so Accept returns a real error that surfaces to the caller.
func TestAcceptError(t *testing.T) {
	ln, err := Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	Run(func(task *Task) (any, error) {
		acc := task.Async(func(ct *Task) (any, error) {
			_, err := ln.Accept(ct)
			return nil, err
		})
		task.Yield()
		ln.Close() // not a task Stop: Accept returns an error, task is not cancelled
		_, err := acc.Wait(task)
		if err == nil {
			t.Errorf("expected accept error")
		}
		if !acc.FailedQ() {
			t.Errorf("state = %v", acc.State())
		}
		return nil, nil
	})
}

// TestConnectError dials a port with no listener, covering Connect's error path.
func TestConnectError(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	probe.Close() // the port is now free: a dial to it is refused
	Run(func(task *Task) (any, error) {
		_, err := Connect(task, "tcp", addr)
		if err == nil {
			t.Errorf("expected connect error")
		}
		return nil, nil
	})
}

// TestListenError covers Listen's error path.
func TestListenError(t *testing.T) {
	if _, err := Listen("tcp", "invalid host:0"); err == nil {
		t.Errorf("expected listen error")
	}
}
