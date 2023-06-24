package ctask

import (
	"context"
	"log"
	"time"
)

type Server struct {
	broker Broker
	mux    map[string]func(context.Context, *Task) error
	sema   chan struct{}
}

func NewServer(broker Broker, concurrency int) *Server {
	return &Server{
		broker: broker,
		mux:    make(map[string]func(context.Context, *Task) error),
		sema:   make(chan struct{}, concurrency),
	}
}

func (s *Server) HandleFunc(pattern string, handler func(context.Context, *Task) error) {
	s.mux[pattern] = handler
}

func (s *Server) Run(ctx context.Context) {
	log.Print("server running...")
	for {
		select {
		case s.sema <- struct{}{}:
			task, err := s.broker.DequeueOneTask("default")
			if err != nil {
				log.Printf("dequeue task error, err: %s", err)
				<-s.sema
				continue
			}
			if task == nil {
				<-s.sema
				continue
			}
			handler := s.mux[task.Name]

			go func() {
				defer func() {
					<-s.sema
				}()

				s.handle(ctx, task, handler)
			}()
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
		s.broker.FailOneTask("default", task.ID, time.Now().Add(task.Retention))
		return
	}

	log.Printf("task [%s] succeeded", task.ID)
	s.broker.SucceedOneTask("default", task.ID, time.Now().Add(task.Retention))
}
