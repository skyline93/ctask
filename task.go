package ctask

import (
	"time"

	"github.com/google/uuid"
)

const (
	TaskStatusQueue     = "queued"
	TaskStatusRunning   = "running"
	TaskStatusSucceeded = "succeeded"
	TaskStatusFailed    = "failed"
)

type Task struct {
	ID        string
	Name      string
	Params    []byte
	Retention time.Duration
	State     string
	Queue     Queue
}

func NewTask(name string, params []byte) *Task {
	return &Task{ID: uuid.New().String(), Name: name, Params: params}
}
