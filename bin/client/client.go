package main

import (
	"context"
	"ctask"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

type EmailTaskPayload struct {
	JobID      string `json:"job_id"`
	OtherParam string `json:"other_param"`
}

func main() {
	ctx := context.Background()
	broker := ctask.NewRDSBroker(ctx, &redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	payload := EmailTaskPayload{
		JobID:      "123456789",
		OtherParam: "hello world",
	}

	v, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 500; i++ {
		task := ctask.NewTask("emailTask", v)
		if err := broker.EnqueueOneTask("default", task, time.Second*60*5); err != nil {
			panic(err)
		}
	}
}
