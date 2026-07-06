package async

// Waiter spawns a group of tasks under a parent and waits for a chosen number of
// them, mirroring Async::Waiter. Unlike Barrier it returns the tasks' results.
type Waiter struct {
	parent *Task
	tasks  []*Task
}

// NewWaiter returns a waiter that spawns its tasks as children of parent.
func NewWaiter(parent *Task) *Waiter { return &Waiter{parent: parent} }

// Async spawns body under the waiter's parent and tracks it (Async::Waiter#async).
func (w *Waiter) Async(body Body) *Task {
	c := w.parent.Async(body)
	w.tasks = append(w.tasks, c)
	return c
}

// Wait joins the first count tracked tasks (all of them when count is negative
// or exceeds the number tracked) and returns their results in order, mirroring
// Async::Waiter#wait(count). It returns the first failure encountered.
func (w *Waiter) Wait(caller *Task, count int) ([]any, error) {
	if count < 0 || count > len(w.tasks) {
		count = len(w.tasks)
	}
	results := make([]any, 0, count)
	for i := 0; i < count; i++ {
		res, err := w.tasks[i].Wait(caller)
		if err != nil {
			return results, err
		}
		results = append(results, res)
	}
	return results, nil
}

// Size returns the number of tracked tasks.
func (w *Waiter) Size() int { return len(w.tasks) }
