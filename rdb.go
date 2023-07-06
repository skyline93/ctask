package ctask

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const globPrefix = "ctask"

var ErrEmptyQueue = errors.New("empty queue")

type RDBBroker struct {
	ctx context.Context
	rdb *redis.Client
}

func NewRDBBroker(ctx context.Context, opt *redis.Options) *RDBBroker {
	client := redis.NewClient(opt)

	return &RDBBroker{ctx: ctx, rdb: client}
}

func (b *RDBBroker) Close() error {
	return b.rdb.Close()
}

var enqueueCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	return -1
end

redis.call("HSET", KEYS[1],
		   "msg", ARGV[1],
		   "state", "queued",
		   "enqueue_since", ARGV[2])
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3])
return 0
`)

func (b *RDBBroker) Enqueue(ctx context.Context, msg *TaskMessage) error {
	encoded, err := EncodeMessage(msg)
	if err != nil {
		return err
	}

	keys := []string{
		fmt.Sprintf("%s:{%s}:task:%s", globPrefix, msg.Queue, msg.ID),
		fmt.Sprintf("%s:{%s}:%s", globPrefix, msg.Queue, TaskStatusQueue),
	}
	args := []interface{}{
		encoded,
		time.Now().UnixNano(),
		msg.ID,
	}
	result, err := enqueueCmd.Run(b.ctx, b.rdb, keys, args...).Result()
	if err != nil {
		return err
	}

	if result.(int64) == -1 {
		return errors.New("task exists already")
	}

	return nil
}

func EncodeMessage(msg *TaskMessage) ([]byte, error) {
	return json.Marshal(msg)
}

func DecodeMessage(data []byte) (*TaskMessage, error) {
	msg := &TaskMessage{}
	if err := json.Unmarshal(data, msg); err != nil {
		return nil, err
	}

	return msg, nil
}

var dequeueCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then
	return nil
end

local mem = redis.call("ZPOPMIN", KEYS[1])
redis.call("ZADD", KEYS[2], ARGV[1], mem[1])

local key = ARGV[2] .. mem[1]
redis.call("HSET", key,
           "state", "running")
local result = redis.call("HMGET", key, "msg", "state", "enqueue_since")

return result
`)

func (b *RDBBroker) Dequeue(qnames ...string) (*TaskMessage, error) {
	for _, qname := range qnames {
		msg, err := b.dequeue(qname)
		if err != nil {
			return nil, err
		}

		if msg == nil {
			continue
		}

		return msg, nil
	}

	return nil, ErrEmptyQueue
}

func (b *RDBBroker) dequeue(qname string) (*TaskMessage, error) {
	keys := []string{
		fmt.Sprintf("%s:{%s}:%s", globPrefix, qname, TaskStatusQueue),
		fmt.Sprintf("%s:{%s}:%s", globPrefix, qname, TaskStatusRunning),
	}

	args := []interface{}{
		float64(time.Now().Unix()),
		fmt.Sprintf("%s:{%s}:task:", globPrefix, qname),
	}

	res, err := dequeueCmd.Run(b.ctx, b.rdb, keys, args...).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	if err == redis.Nil {
		return nil, nil
	}

	encoded := res.([]interface{})
	return DecodeMessage([]byte(encoded[0].(string)))
}

var completeCmd = redis.NewScript(`
if redis.call("ZSCORE", KEYS[1], ARGV[1]) == false then
	return -1
end

redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("HSET", KEYS[2],
           "state", ARGV[2])
redis.call("EXPIREAT", KEYS[2], ARGV[3])
return 0
`)

func (b *RDBBroker) completeTask(taskId string, status string, expireat time.Time, qname string) error {
	keys := []string{
		fmt.Sprintf("%s:{%s}:%s", globPrefix, qname, TaskStatusRunning),
		fmt.Sprintf("%s:{%s}:task:%s", globPrefix, qname, taskId),
	}

	args := []interface{}{
		taskId,
		status,
		expireat.Unix(),
	}

	result, err := completeCmd.Run(b.ctx, b.rdb, keys, args...).Result()
	if err != nil {
		return err
	}

	if result.(int64) == -1 {
		return errors.New("task not exists")
	}

	return nil
}

func (b *RDBBroker) SucceedTask(taskId string, expireat time.Time, qname string) error {
	return b.completeTask(taskId, TaskStatusSucceeded, expireat, qname)
}

func (b *RDBBroker) FailTask(taskId string, expireat time.Time, qname string) error {
	return b.completeTask(taskId, TaskStatusFailed, expireat, qname)
}
