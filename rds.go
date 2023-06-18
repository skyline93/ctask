package ctask

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const globPrefix = "worker"

type RDS struct {
	client *redis.Client
}

func NewRDS(opt *redis.Options) *RDS {
	client := redis.NewClient(opt)
	return &RDS{client: client}
}

type RDSBroker struct {
	ctx context.Context
	rds *RDS
}

func NewRDSBroker(ctx context.Context, opt *redis.Options) *RDSBroker {
	rds := NewRDS(opt)
	return &RDSBroker{ctx: ctx, rds: rds}
}

func (b *RDSBroker) InitQueues(queues ...string) error {
	for _, q := range queues {
		k := fmt.Sprintf("%s:queue", globPrefix)
		if cmd := b.rds.client.SAdd(b.ctx, k, q); cmd.Err() != nil {
			return cmd.Err()
		}
	}

	return nil
}

var enqueueOneCmd = redis.NewScript(`
redis.call("HSET", KEYS[1],
		   "id", ARGV[1],
		   "name", ARGV[2],
		   "params", ARGV[3])
redis.call("ZADD", KEYS[2], ARGV[4], ARGV[5])
return 0
`)

func (b *RDSBroker) EnqueueOneTask(queue string, task *Task) error {
	keys := []string{
		fmt.Sprintf("%s:{%s}:task:%s", globPrefix, queue, task.ID),
		fmt.Sprintf("%s:{%s}:%s", globPrefix, queue, TaskStatusQueue),
	}
	args := []interface{}{
		task.ID,
		task.Name,
		task.Params,
		float64(time.Now().Unix()),
		task.ID,
	}
	if _, err := enqueueOneCmd.Run(b.ctx, b.rds.client, keys, args...).Result(); err != nil {
		return err
	}

	return nil
}

var dequeueOneCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then
	return nil
end

local mem = redis.call("ZPOPMIN", KEYS[1])
redis.call("ZADD", KEYS[2], ARGV[1], mem[1])

local key = ARGV[2] .. mem[1]
local result = redis.call("HMGET", key, "id", "name", "params")

return result
`)

func (b *RDSBroker) DequeueOneTask(queue string) (*Task, error) {
	keys := []string{
		fmt.Sprintf("%s:{%s}:%s", globPrefix, queue, TaskStatusQueue),
		fmt.Sprintf("%s:{%s}:%s", globPrefix, queue, TaskStatusRunning),
	}

	args := []interface{}{
		float64(time.Now().Unix()),
		fmt.Sprintf("%s:{%s}:task:", globPrefix, queue),
	}

	result, err := dequeueOneCmd.Run(b.ctx, b.rds.client, keys, args...).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	if err == redis.Nil {
		return nil, nil
	}

	res := result.([]interface{})

	return &Task{
		ID:     res[0].(string),
		Name:   res[1].(string),
		Params: []byte(res[2].(string)),
	}, nil
}

var completeOneCmd = redis.NewScript(`
if redis.call("ZSCORE", KEYS[1], ARGV[1]) == false then
	return -1
end

redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])

return 0
`)

func (b *RDSBroker) completeOnTask(queue string, taskId string, status string) error {
	keys := []string{
		fmt.Sprintf("%s:{%s}:%s", globPrefix, queue, TaskStatusRunning),
		fmt.Sprintf("%s:{%s}:%s", globPrefix, queue, status),
	}

	args := []interface{}{
		taskId,
		float64(time.Now().Unix()),
	}

	result, err := completeOneCmd.Run(b.ctx, b.rds.client, keys, args...).Result()
	if err != nil {
		return err
	}

	if result.(int64) == -1 {
		return errors.New("task not exists")
	}

	return nil
}

func (b *RDSBroker) SucceedOneTask(queue string, taskId string) error {
	return b.completeOnTask(queue, taskId, TaskStatusSucceeded)
}

func (b *RDSBroker) FailOneTask(queue string, taskId string) error {
	return b.completeOnTask(queue, taskId, TaskStatusFailed)
}
