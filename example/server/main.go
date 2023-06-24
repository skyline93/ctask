package main

import (
	"app/tasks"
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/skyline93/ctask"
)

func main() {
	ctx := context.Background()
	broker := ctask.NewRDSBroker(ctx, &redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	srv := ctask.NewServer(broker, ctask.Config{
		Concurrency: 5,
		Queues: map[string]int{
			"critical": 6,
			"default":  3,
			"low":      1,
		},
	})
	srv.HandleFunc(tasks.TypeEmailDelivery, tasks.HandleEmailDeliveryTask)

	srv.Run(ctx)
}
