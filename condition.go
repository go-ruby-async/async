package async

// condWaiter is one task parked in Condition.Wait, together with the value it
// will receive when signalled.
type condWaiter struct {
	task  *Task
	value any
}

// Condition is a synchronization point where tasks wait to be signalled,
// mirroring Async::Condition. A signal wakes every waiting task, optionally
// delivering a value that each waiter's Wait returns.
type Condition struct {
	waiters []*condWaiter
}

// NewCondition returns a condition with no waiters.
func NewCondition() *Condition { return &Condition{} }

// Wait suspends the calling task until the condition is signalled, returning the
// signalled value (Async::Condition#wait).
func (c *Condition) Wait(t *Task) any {
	w := &condWaiter{task: t}
	c.waiters = append(c.waiters, w)
	t.suspend(func() { c.remove(w) })
	return w.value
}

// Signal wakes every waiting task, delivering value to each (Async::Condition#signal).
func (c *Condition) Signal(value any) {
	ws := c.waiters
	c.waiters = nil
	for _, w := range ws {
		w.value = value
		w.task.wake()
	}
}

// WaitCount returns the number of tasks currently waiting.
func (c *Condition) WaitCount() int { return len(c.waiters) }

// Empty reports whether no task is waiting (Async::Condition#empty?).
func (c *Condition) Empty() bool { return len(c.waiters) == 0 }

func (c *Condition) remove(w *condWaiter) {
	for i, x := range c.waiters {
		if x == w {
			c.waiters = append(c.waiters[:i], c.waiters[i+1:]...)
			return
		}
	}
}

// Notification is a one-way wakeup: a producer signals a consumer waiting on it,
// mirroring Async::Notification (a Condition specialised to valueless wakeups).
type Notification struct {
	cond Condition
}

// NewNotification returns a notification with no waiters.
func NewNotification() *Notification { return &Notification{} }

// Wait suspends the calling task until the notification is signalled
// (Async::Notification#wait).
func (n *Notification) Wait(t *Task) { n.cond.Wait(t) }

// Signal wakes every task waiting on the notification (Async::Notification#signal).
func (n *Notification) Signal() { n.cond.Signal(nil) }

// WaitCount returns the number of tasks currently waiting.
func (n *Notification) WaitCount() int { return n.cond.WaitCount() }
