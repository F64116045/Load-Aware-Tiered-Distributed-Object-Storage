package tiering

import (
	"context"
	"fmt"
	"log"
	"time"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/meta"
)

const (
	TaskTypeReplicationToEC = "REPL_TO_EC"
	TaskTypeRepair          = "REPAIR"
	TaskTypeGC              = "GC"
	TaskTypeOldVersionGC    = "GC_OLD_VERSION"
)

// Processor encapsulates task business logic.
type Processor interface {
	ProcessReplicationToEC(ctx context.Context, task *meta.TieringTask) error
	ProcessReplicationRepair(ctx context.Context, task *meta.TieringTask) error
	ProcessReplicationGC(ctx context.Context, task *meta.TieringTask) error
	ProcessOldVersionGC(ctx context.Context, task *meta.TieringTask) error
}

// Worker polls tasks and executes processor logic.
type Worker struct {
	store        meta.Repository
	processor    Processor
	pollInterval time.Duration
	taskType     string
}

// NewWorker constructs a worker with sane defaults.
func NewWorker(store meta.Repository, processor Processor, pollInterval time.Duration, taskType string) *Worker {
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	return &Worker{
		store:        store,
		processor:    processor,
		pollInterval: pollInterval,
		taskType:     taskType,
	}
}

// Run starts the worker loop until context cancellation.
func (w *Worker) Run(ctx context.Context) error {
	if w.store == nil {
		return fmt.Errorf("tiering worker store is nil")
	}
	if w.processor == nil {
		return fmt.Errorf("tiering worker processor is nil")
	}

	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		if err := w.runOnce(ctx); err != nil {
			log.Printf("[TieringWorker] runOnce failed: %v", err)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (w *Worker) runOnce(ctx context.Context) error {
	task, err := w.store.ClaimNextTieringTask(ctx, w.taskType)
	if err != nil {
		return err
	}
	if task == nil {
		return nil
	}

	var procErr error
	switch task.TaskType {
	case TaskTypeReplicationToEC:
		procErr = w.processor.ProcessReplicationToEC(ctx, task)
	case TaskTypeRepair:
		procErr = w.processor.ProcessReplicationRepair(ctx, task)
	case TaskTypeGC:
		procErr = w.processor.ProcessReplicationGC(ctx, task)
	case TaskTypeOldVersionGC:
		procErr = w.processor.ProcessOldVersionGC(ctx, task)
	default:
		procErr = fmt.Errorf("unsupported task type: %s", task.TaskType)
	}

	if procErr == nil {
		return w.store.MarkTieringTaskDone(ctx, task.TaskID)
	}

	nextRetryCount := task.RetryCount + 1
	if nextRetryCount >= config.TieringTaskMaxRetryCount {
		log.Printf(
			"[TieringWorker] task=%s type=%s reached retry cap (%d/%d), mark FAILED: %v",
			task.TaskID,
			task.TaskType,
			nextRetryCount,
			config.TieringTaskMaxRetryCount,
			procErr,
		)
		return w.store.MarkTieringTaskFailed(ctx, task.TaskID, procErr.Error())
	}

	backoff := retryBackoff(task.RetryCount)
	return w.store.MarkTieringTaskRetry(
		ctx,
		task.TaskID,
		procErr.Error(),
		time.Now().Add(backoff),
	)
}

func retryBackoff(retryCount int) time.Duration {
	if retryCount < 0 {
		retryCount = 0
	}
	// 2s, 4s, 8s ... cap at 5 minutes
	backoff := 2 * time.Second * time.Duration(1<<retryCount)
	if backoff > 5*time.Minute {
		return 5 * time.Minute
	}
	return backoff
}
