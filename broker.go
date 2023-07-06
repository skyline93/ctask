package ctask

import (
	"context"
	"time"
)

type Broker interface {
	Close()	error
	Enqueue(ctx context.Context, msg *TaskMessage) error
	Dequeue(qnames ...string) (*TaskMessage, error)
	SucceedTask(taskId string, expireat time.Time, qname string) error
	FailTask(taskId string, expireat time.Time, qnmae string) error
}

type TaskMessage struct {
	ID        string
	Type      string
	Payload   []byte
	Queue     string
	Retention int64
}
