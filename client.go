package ctask

import (
	"context"
	"time"
)

type Client struct {
	broker Broker
}

func NewClient(broker Broker) *Client {
	return &Client{broker: broker}
}

func (c *Client) Close() error { return c.broker.Close() }

func (c *Client) Enqueue(ctx context.Context, task *Task, opts ...Option) (*TaskInfo, error) {
	opt, err := composeOptions(opts...)
	if err != nil {
		return nil, err
	}

	msg := TaskMessage{
		ID:        opt.taskID,
		Type:      task.Type(),
		Payload:   task.Payload(),
		Queue:     opt.queue,
		Retention: int64(opt.retention.Seconds()),
	}

	err = c.broker.Enqueue(ctx, &msg)
	if err != nil {
		return nil, err
	}

	return &TaskInfo{
		ID:        msg.ID,
		Type:      msg.Type,
		Queue:     msg.Queue,
		Retention: time.Duration(msg.Retention),
	}, nil
}
