package ctask

import "time"

type Broker interface {
	InitQueues(queues ...string) error
	EnqueueOneTask(queue string, task *Task, retention time.Duration) error
	DequeueOneTask(queue string) (*Task, error)
	SucceedOneTask(queue string, taskId string, expireat time.Time) error
	FailOneTask(queue string, taskId string, expireat time.Time) error
}
