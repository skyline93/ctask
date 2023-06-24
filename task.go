package ctask

import "time"

const (
	TaskStatusQueue     = "queued"
	TaskStatusRunning   = "running"
	TaskStatusSucceeded = "succeeded"
	TaskStatusFailed    = "failed"
)

type Task struct {
	typename string
	payload  []byte
	opts     []Option
}

func (t *Task) Payload() []byte { return t.payload }
func (t *Task) Type() string    { return t.typename }

func NewTask(typename string, payload []byte, opts ...Option) *Task {
	return &Task{
		typename: typename,
		payload:  payload,
		opts:     opts,
	}
}

func newTask(typename string, payload []byte) *Task {
	return &Task{
		typename: typename,
		payload:  payload,
	}
}

type TaskInfo struct {
	ID        string
	Type      string
	Queue     string
	Retention time.Duration
}
