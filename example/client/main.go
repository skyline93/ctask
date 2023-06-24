package main

import (
	"context"
	"log"
	"time"

	"app/tasks"

	"github.com/redis/go-redis/v9"
	"github.com/skyline93/ctask"
)

func main() {
	ctx := context.Background()
	broker := ctask.NewRDSBroker(ctx, &redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	client := ctask.NewClient(broker)

	task, err := tasks.NewEmailDeliveryTask(42, "some:template:id")
	if err != nil {
		log.Fatalf("could not create task: %v", err)
	}

	queues := []string{"critical", "default", "low"}
	for _, qname := range queues {
		for i := 0; i < 10; i++ {
			info, err := client.Enqueue(ctx, task, ctask.Queue(qname), ctask.Retention(time.Second*60))
			if err != nil {
				log.Fatalf("could not enqueue task: %v", err)
				return
			}
			log.Printf("enqueued task: id=%s queue=%s", info.ID, info.Queue)
		}
	}
}
