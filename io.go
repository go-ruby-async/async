package async

import (
	"context"
	"errors"
	"net"
	"time"
)

// ErrNoIO is returned by the Async IO wrappers when the task's Scheduler does
// not implement IOScheduler, so it cannot host the non-blocking IO reactor. The
// bundled CoScheduler always can; a custom Scheduler must implement AwaitIO.
var ErrNoIO = errors.New("async: scheduler does not support IO")

// Socket is the reactor-aware wrapper around a net.Conn, mirroring the gem's
// Async::IO::Socket / Async::IO::Stream: its Read and Write suspend the calling
// fiber back to the reactor while the real syscall runs on a goroutine, instead
// of blocking a thread. Go's runtime poller gives us genuine non-blocking IO, so
// each operation is a single goroutine plus a fiber park.
//
// A Socket is bound to the task that performs each operation (passed explicitly,
// as the gem passes Async::Task.current), so a Stop or a with_timeout on that
// task cancels an in-flight Read/Write: the reactor sets a past deadline on the
// underlying connection, the syscall returns, and the fiber unwinds.
type Socket struct {
	conn net.Conn
}

// Wrap adapts an existing net.Conn (a pipe end, an accepted connection, a dialed
// socket) into a reactor-aware Socket. In-process net.Pipe and loopback TCP both
// work, which is what the tests use — no external network.
func Wrap(conn net.Conn) *Socket { return &Socket{conn: conn} }

// Conn returns the underlying net.Conn.
func (s *Socket) Conn() net.Conn { return s.conn }

// Read reads into p, suspending t's fiber on the reactor until the read
// completes; it returns the number of bytes read and any error, mirroring a
// non-blocking Async::IO read. A Stop or timeout on t while the read is
// outstanding unblocks it via a past read deadline and unwinds the fiber.
func (s *Socket) Read(t *Task, p []byte) (int, error) {
	_ = s.conn.SetReadDeadline(time.Time{}) // clear any deadline left by a prior cancel
	var n int
	var err error
	if ioErr := t.awaitIO(func() {
		n, err = s.conn.Read(p)
	}, func() {
		_ = s.conn.SetReadDeadline(time.Now())
	}); ioErr != nil {
		return 0, ioErr
	}
	return n, err
}

// Write writes p, suspending t's fiber on the reactor until the write completes;
// it returns the number of bytes written and any error. A Stop or timeout on t
// unblocks an outstanding write via a past write deadline.
func (s *Socket) Write(t *Task, p []byte) (int, error) {
	_ = s.conn.SetWriteDeadline(time.Time{})
	var n int
	var err error
	if ioErr := t.awaitIO(func() {
		n, err = s.conn.Write(p)
	}, func() {
		_ = s.conn.SetWriteDeadline(time.Now())
	}); ioErr != nil {
		return 0, ioErr
	}
	return n, err
}

// Close closes the underlying connection (Async::IO#close).
func (s *Socket) Close() error { return s.conn.Close() }

// Listener is the reactor-aware wrapper around a net.Listener, mirroring
// Async::IO::Endpoint#accept: its Accept suspends the calling fiber until an
// inbound connection arrives, returning it as a Socket.
type Listener struct {
	ln net.Listener
}

// Listen opens a listening socket and wraps it for the reactor. It is not itself
// a blocking operation, so it does not park a fiber; use 127.0.0.1:0 for an
// in-process loopback listener with an OS-assigned port.
func Listen(network, addr string) (*Listener, error) {
	ln, err := net.Listen(network, addr)
	if err != nil {
		return nil, err
	}
	return &Listener{ln: ln}, nil
}

// WrapListener adapts an existing net.Listener into a reactor-aware Listener.
func WrapListener(ln net.Listener) *Listener { return &Listener{ln: ln} }

// Addr returns the listener's network address (Async::IO::Endpoint#local_address).
func (l *Listener) Addr() net.Addr { return l.ln.Addr() }

// Accept waits for the next inbound connection, suspending t's fiber on the
// reactor until one arrives, and returns it as a Socket. A Stop or timeout on t
// while Accept is outstanding cancels it: the reactor sets a past deadline on
// the listener when it supports one, else closes it, so Accept returns and the
// fiber unwinds.
func (l *Listener) Accept(t *Task) (*Socket, error) {
	var conn net.Conn
	var err error
	if ioErr := t.awaitIO(func() {
		conn, err = l.ln.Accept()
	}, l.cancelAccept); ioErr != nil {
		return nil, ioErr
	}
	if err != nil {
		return nil, err
	}
	return &Socket{conn: conn}, nil
}

// cancelAccept unblocks a parked Accept. A *net.TCPListener (and most real
// listeners) supports SetDeadline; a listener that does not is closed instead.
func (l *Listener) cancelAccept() {
	if d, ok := l.ln.(interface{ SetDeadline(time.Time) error }); ok {
		_ = d.SetDeadline(time.Now())
		return
	}
	_ = l.ln.Close()
}

// Close closes the listener (Async::IO::Endpoint#close).
func (l *Listener) Close() error { return l.ln.Close() }

// Connect dials network/addr, suspending t's fiber on the reactor until the
// connection is established, and returns it as a Socket. A Stop or timeout on t
// while the dial is outstanding cancels it through the dial context. It mirrors
// Async::IO::Endpoint#connect.
func Connect(t *Task, network, addr string) (*Socket, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var conn net.Conn
	var err error
	if ioErr := t.awaitIO(func() {
		conn, err = (&net.Dialer{}).DialContext(ctx, network, addr)
	}, cancel); ioErr != nil {
		return nil, ioErr
	}
	if err != nil {
		return nil, err
	}
	return &Socket{conn: conn}, nil
}
