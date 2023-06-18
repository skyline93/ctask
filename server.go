package ctask

import (
	"context"
	"log"
	"time"
)

type Server struct {
	broker Broker
	mux    map[string]func(context.Context, *Task) error
}

func NewServer(broker Broker) *Server {
	return &Server{
		broker: broker,
		mux:    make(map[string]func(context.Context, *Task) error),
	}
}

func (s *Server) HandleFunc(pattern string, handler func(context.Context, *Task) error) {
	s.mux[pattern] = handler
}

func (s *Server) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)

	log.Print("server running...")
	for {
		select {
		case <-ticker.C:
			// TODO 实现worker pool，每个worker不断从队列中获取任务执行
			task, err := s.broker.DequeueOneTask("default")
			if err != nil {
				log.Printf("dequeue task error, err: %s", err)
				continue
			}
			if task == nil {
				continue
			}
			handler := s.mux[task.Name]

			go s.handle(ctx, task, handler)
		case <-ctx.Done():
			log.Print("stop server")
		}
	}
}

func (s *Server) handle(ctx context.Context, task *Task, handler func(context.Context, *Task) error) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("handle panic!!! %s", err)
			return
		}
	}()

	log.Printf("task [%s] started", task.ID)

	if err := handler(ctx, task); err != nil {
		log.Printf("task [%s] failed", task.ID)
		s.broker.FailOneTask("default", task.ID)
		return
	}

	log.Printf("task [%s] succeeded", task.ID)
	s.broker.SucceedOneTask("default", task.ID)
}
