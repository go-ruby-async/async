package async

// Barrier collects tasks and waits for them all, mirroring Async::Barrier. Tasks
// spawned through the barrier's Async are tracked so a single Wait joins them in
// order, and Stop cancels the whole group.
type Barrier struct {
	tasks []*Task
}

// NewBarrier returns an empty barrier.
func NewBarrier() *Barrier { return &Barrier{} }

// Async spawns body as a child of parent and adds it to the barrier
// (Async::Barrier#async).
func (b *Barrier) Async(parent *Task, body Body) *Task {
	c := parent.Async(body)
	b.tasks = append(b.tasks, c)
	return c
}

// Wait joins every tracked task in turn, returning the first failure it
// encounters (ErrStop for a stopped task) and leaving the remaining tasks in the
// barrier; on success the barrier is emptied (Async::Barrier#wait).
func (b *Barrier) Wait(caller *Task) error {
	for len(b.tasks) > 0 {
		t := b.tasks[0]
		if _, err := t.Wait(caller); err != nil {
			b.tasks = b.tasks[1:]
			return err
		}
		b.tasks = b.tasks[1:]
	}
	return nil
}

// Stop cancels every tracked task and clears the barrier (Async::Barrier#stop).
func (b *Barrier) Stop() {
	for _, t := range b.tasks {
		t.Stop()
	}
	b.tasks = nil
}

// Size returns the number of tracked tasks (Async::Barrier#size).
func (b *Barrier) Size() int { return len(b.tasks) }

// Empty reports whether the barrier tracks no tasks (Async::Barrier#empty?).
func (b *Barrier) Empty() bool { return len(b.tasks) == 0 }
