package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/skyline93/ctask"
	"app/tasks"
)

func main() {
	ctx := context.Background()
	broker := ctask.NewRDSBroker(ctx, &redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	payload := tasks.EmailTaskPayload{
		JobID:      "123456789",
		OtherParam: "hello world",
	}

	v, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 100; i++ {
		task := ctask.NewTask("emailTask", v)
		if err := broker.EnqueueTask(task, time.Second*60*5, ctask.Queue{Name: "default"}); err != nil {
			panic(err)
		}
	}

	for i := 0; i < 100; i++ {
		task := ctask.NewTask("emailTask", v)
		if err := broker.EnqueueTask(task, time.Second*60*5, ctask.Queue{Name: "low"}); err != nil {
			panic(err)
		}
	}
	for i := 0; i < 100; i++ {
		task := ctask.NewTask("emailTask", v)
		if err := broker.EnqueueTask(task, time.Second*60*5, ctask.Queue{Name: "critical"}); err != nil {
			panic(err)
		}
	}
}
