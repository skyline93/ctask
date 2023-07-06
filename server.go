package ctask

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sort"
	"time"
)

type Config struct {
	Concurrency    int
	Queues         map[string]int
	StrictPriority bool
}

type Server struct {
	broker         Broker
	mux            map[string]Handler
	sema           chan struct{}
	queueConfig    map[string]int
	strictPriority bool
}

func NewServer(broker Broker, cfg Config) *Server {
	queues := make(map[string]int)
	for qname, priority := range cfg.Queues {
		queues[qname] = priority
	}

	return &Server{
		broker:         broker,
		mux:            make(map[string]Handler),
		sema:           make(chan struct{}, cfg.Concurrency),
		queueConfig:    queues,
		strictPriority: cfg.StrictPriority,
	}
}

func (s *Server) HandleFunc(pattern string, handler func(context.Context, *Task) error) {
	s.mux[pattern] = HandlerFunc(handler)
}

func (s *Server) Handle(pattern string, handler Handler) {
	s.mux[pattern] = handler
}

func uniq(names []string, l int) []string {
	var res []string
	seen := make(map[string]struct{})
	for _, s := range names {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			res = append(res, s)
		}
		if len(res) == l {
			break
		}
	}
	return res
}

func (s *Server) weightedQueues() []string {
	var names []string

	for qname, priority := range s.queueConfig {
		for i := 0; i < priority; i++ {
			names = append(names, qname)
		}
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
	return uniq(names, len(s.queueConfig))
}

func (s *Server) strictQueues() []string {
	names := make([]string, 0, len(s.queueConfig))

	for qname := range s.queueConfig {
		names = append(names, qname)
	}

	sort.SliceStable(names, func(i, j int) bool {
		return s.queueConfig[names[i]] > s.queueConfig[names[j]]
	})

	return names
}

func (s *Server) queues() []string {
	if s.strictPriority {
		return s.strictQueues()
	}

	return s.weightedQueues()
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

func (s *Server) handle(ctx context.Context, msg *TaskMessage, handler Handler) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("handle panic!!! %s", err)
			return
		}
	}()

	task := newTask(msg.Type, msg.Payload)
	if err := handler.ProcessTask(ctx, task); err != nil {
		log.Printf("task [%s] failed", msg.ID)
		s.broker.FailTask(msg.ID, time.Now().Add(time.Second*time.Duration(msg.Retention)), msg.Queue)
		return
	}

	log.Printf("task [%s] succeed", msg.ID)
	s.broker.SucceedTask(msg.ID, time.Now().Add(time.Second*time.Duration(msg.Retention)), msg.Queue)
}
