package main

import (
	"app/tasks"
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/skyline93/ctask"
)

func main() {
	ctx := context.Background()
	broker := ctask.NewRDBBroker(ctx, &redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})

	srv := ctask.NewServer(broker, ctask.Config{
		Concurrency: 100,
		Queues: map[string]int{
			"critical": 6,
			"default":  3,
			"low":      1,
		},
	})
	srv.HandleFunc(tasks.TypeEmailDelivery, tasks.HandleEmailDeliveryTask)
	srv.Handle(tasks.TypeImageResize, tasks.NewImageProcessor())

	srv.Run(ctx)
}
