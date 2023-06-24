package ctask

import (
	"context"
	"errors"
	"log"
	"sort"
	"time"
)

type Config struct {
	Concurrency int
	Queues      map[string]int
}

type Server struct {
	broker Broker
	mux    map[string]func(context.Context, *Task) error
	sema   chan struct{}
	queues []Queue
}

func NewServer(broker Broker, cfg Config) *Server {
	var queues []Queue
	for qname, priority := range cfg.Queues {
		queues = append(queues, Queue{Name: qname, priority: priority})
	}

	sort.Slice(queues, func(i, j int) bool {
		return queues[i].priority > queues[j].priority
	})

	return &Server{
		broker: broker,
		mux:    make(map[string]func(context.Context, *Task) error),
		sema:   make(chan struct{}, cfg.Concurrency),
		queues: queues,
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
			task, err := s.broker.DequeueTask(s.queues...)
			if err != nil && !errors.Is(err, ErrEmptyQueue) {
				log.Printf("dequeue task error, err: %s", err)
				<-s.sema
				break
			} else if errors.Is(err, ErrEmptyQueue) {
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
			return
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
		s.broker.FailTask(task.ID, time.Now().Add(task.Retention), task.Queue)
		return
	}

	log.Printf("task [%s] succeeded", task.ID)
	s.broker.SucceedTask(task.ID, time.Now().Add(task.Retention), task.Queue)
}
