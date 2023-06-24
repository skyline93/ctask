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
	broker      Broker
	mux         map[string]func(context.Context, *Task) error
	sema        chan struct{}
	queueConfig map[string]int
}

func NewServer(broker Broker, cfg Config) *Server {
	queues := make(map[string]int)
	for qname, priority := range cfg.Queues {
		queues[qname] = priority
	}

	return &Server{
		broker:      broker,
		mux:         make(map[string]func(context.Context, *Task) error),
		sema:        make(chan struct{}, cfg.Concurrency),
		queueConfig: queues,
	}
}

func (s *Server) HandleFunc(pattern string, handler func(context.Context, *Task) error) {
	s.mux[pattern] = handler
}

func (s *Server) queues() []string {
	names := make([]string, 0, len(s.queueConfig))

	for qname := range s.queueConfig {
		names = append(names, qname)
	}

	sort.SliceStable(names, func(i, j int) bool {
		return s.queueConfig[names[i]] > s.queueConfig[names[j]]
	})

	return names
}

func (s *Server) Run(ctx context.Context) {
	log.Print("server running...")
	for {
		select {
		case s.sema <- struct{}{}:
			qnames := s.queues()
			msg, err := s.broker.Dequeue(qnames...)
			if err != nil && !errors.Is(err, ErrEmptyQueue) {
				log.Printf("dequeue task error, err: %s", err)
				<-s.sema
				break
			} else if errors.Is(err, ErrEmptyQueue) {
				time.Sleep(time.Second)
				<-s.sema
				continue
			}

			go func() {
				defer func() {
					<-s.sema
				}()

				handler := s.mux[msg.Type]
				s.handle(ctx, msg, handler)
			}()
		case <-ctx.Done():
			log.Print("stop server")
			return
		}
	}
}

func (s *Server) handle(ctx context.Context, msg *TaskMessage, handler func(context.Context, *Task) error) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("handle panic!!! %s", err)
			return
		}
	}()

	task := newTask(msg.Type, msg.Payload)
	if err := handler(ctx, task); err != nil {
		log.Printf("task [%s] failed", msg.ID)
		s.broker.FailTask(msg.ID, time.Now().Add(time.Second*time.Duration(msg.Retention)), msg.Queue)
		return
	}

	log.Printf("task [%s] succeed", msg.ID)
	s.broker.SucceedTask(msg.ID, time.Now().Add(time.Second*time.Duration(msg.Retention)), msg.Queue)
}
