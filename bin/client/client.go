package main

import (
	"context"
	"ctask"
	"encoding/json"

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

	task := ctask.NewTask("emailTask", v)

	if err := broker.EnqueueOneTask("default", task); err != nil {
		panic(err)
	}

	// t, err := broker.DequeueOneTask("default")
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Printf("task: %v", t)

	// if err = broker.FailOneTask("default", "40e87fa3-7cf7-42d9-a556-d1667f63d7af"); err != nil {
	// 	panic(err)
	// }
}
