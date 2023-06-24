package ctask

import "time"

type Broker interface {
	EnqueueTask(task *Task, retention time.Duration, queue Queue) error
	DequeueTask(queues ...Queue) (*Task, error)
	SucceedTask(taskId string, expireat time.Time, queue Queue) error
	FailTask(taskId string, expireat time.Time, queue Queue) error
}

type Queue struct {
	Name     string
	priority int
}

func (q Queue) String() string {
	return q.Name
}
