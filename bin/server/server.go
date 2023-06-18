package main

import (
	"context"
	"ctask"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var taskName = "emailTask"

func HandleEmailTask(ctx context.Context, t *ctask.Task) error {

	fmt.Printf("handle email task...\n")
	time.Sleep(time.Second * 5)
	return nil
}

func main() {
	ctx := context.Background()
	broker := ctask.NewRDSBroker(ctx, &redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	srv := ctask.NewServer(broker)
	srv.HandleFunc(taskName, HandleEmailTask)

	srv.Run(ctx)
}
