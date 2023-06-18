package ctask

type Broker interface {
	InitQueues(queues ...string) error
	EnqueueOneTask(queue string, task *Task) error
	DequeueOneTask(queue string) (*Task, error)
	SucceedOneTask(queue string, taskId string) error
	FailOneTask(queue string, taskId string) error
}
