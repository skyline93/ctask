package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/skyline93/ctask"
)

var TaskName = "emailTask"

type EmailTaskPayload struct {
	JobID      string `json:"job_id"`
	OtherParam string `json:"other_param"`
}

func HandleEmailTask(ctx context.Context, t *ctask.Task) error {

	fmt.Printf("handle email task...\n")
	time.Sleep(time.Second * 5)
	return nil
}
