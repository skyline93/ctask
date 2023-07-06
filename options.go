package ctask

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	DefaultQueue     = "default"
	DefaultRetention = time.Duration(time.Hour * 24)
)

type OptionType int

const (
	QueueOpt OptionType = iota
	RetentionOpt
)

type Option interface {
	String() string
	Type() OptionType
	Value() interface{}
}

type queueOption string

func Queue(q string) Option {
	return queueOption(q)
}

func (q queueOption) String() string     { return fmt.Sprintf("Queue(%s)", string(q)) }
func (q queueOption) Type() OptionType   { return RetentionOpt }
func (q queueOption) Value() interface{} { return string(q) }

type retentionOption time.Duration

func Retention(d time.Duration) Option {
	return retentionOption(d)
}

func (ttl retentionOption) String() string     { return fmt.Sprintf("Retention(%d)", time.Duration(ttl)) }
func (ttl retentionOption) Type() OptionType   { return RetentionOpt }
func (ttl retentionOption) Value() interface{} { return time.Duration(ttl) }

type option struct {
	taskID    string
	queue     string
	retention time.Duration
}

func composeOptions(opts ...Option) (option, error) {
	res := option{
		taskID:    uuid.New().String(),
		queue:     DefaultQueue,
		retention: DefaultRetention,
	}

	for _, opt := range opts {
		switch opt := opt.(type) {
		case queueOption:
			res.queue = string(opt)
		case retentionOption:
			res.retention = time.Duration(opt)
		default:
		}
	}

	return res, nil
}
