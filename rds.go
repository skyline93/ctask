package ctask

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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
if redis.call("EXISTS", KEYS[1]) == 1 then
	return -1
end

redis.call("HSET", KEYS[1],
		   "id", ARGV[1],
		   "name", ARGV[2],
		   "params", ARGV[3],
		   "retention", ARGV[4],
		   "state", "queued")
redis.call("ZADD", KEYS[2], ARGV[5], ARGV[6])
return 0
`)

func (b *RDSBroker) EnqueueOneTask(queue string, task *Task, retention time.Duration) error {
	keys := []string{
		fmt.Sprintf("%s:{%s}:task:%s", globPrefix, queue, task.ID),
		fmt.Sprintf("%s:{%s}:%s", globPrefix, queue, TaskStatusQueue),
	}
	args := []interface{}{
		task.ID,
		task.Name,
		task.Params,
		retention,
		float64(time.Now().Unix()),
		task.ID,
	}
	result, err := enqueueOneCmd.Run(b.ctx, b.rds.client, keys, args...).Result()
	if err != nil {
		return err
	}

	if result.(int64) == -1 {
		return errors.New("task exists already")
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
redis.call("HSET", key,
           "state", "running")
local result = redis.call("HMGET", key, "id", "name", "params", "retention", "state")

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

	retention, err := strconv.ParseInt(res[3].(string), 10, 64)
	if err != nil {
		return nil, err
	}

	return &Task{
		ID:        res[0].(string),
		Name:      res[1].(string),
		Params:    []byte(res[2].(string)),
		Retention: time.Duration(retention),
		State:     res[4].(string),
	}, nil
}

var completeOneCmd = redis.NewScript(`
if redis.call("ZSCORE", KEYS[1], ARGV[1]) == false then
	return -1
end

redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("HSET", KEYS[2],
           "state", ARGV[2])
redis.call("EXPIREAT", KEYS[2], ARGV[3])
return 0
`)

func (b *RDSBroker) completeOnTask(queue string, taskId string, status string, expireat time.Time) error {
	keys := []string{
		fmt.Sprintf("%s:{%s}:%s", globPrefix, queue, TaskStatusRunning),
		fmt.Sprintf("%s:{%s}:task:%s", globPrefix, queue, taskId),
	}

	args := []interface{}{
		taskId,
		status,
		expireat.Unix(),
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

func (b *RDSBroker) SucceedOneTask(queue string, taskId string, expireat time.Time) error {
	return b.completeOnTask(queue, taskId, TaskStatusSucceeded, expireat)
}

func (b *RDSBroker) FailOneTask(queue string, taskId string, expireat time.Time) error {
	return b.completeOnTask(queue, taskId, TaskStatusFailed, expireat)
}
