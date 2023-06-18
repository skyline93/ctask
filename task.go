package ctask

import "github.com/google/uuid"

const (
	TaskStatusQueue     = "queued"
	TaskStatusRunning   = "running"
	TaskStatusSucceeded = "succeeded"
	TaskStatusFailed    = "failed"
)

type Task struct {
	ID     string
	Name   string
	Params []byte
}

func NewTask(name string, params []byte) *Task {
	return &Task{ID: uuid.New().String(), Name: name, Params: params}
}
