package tiering

import (
	"context"
	"fmt"

	"hybrid_distributed_store/internal/meta"
)

// ProcessorMux routes tiering tasks to dedicated processors.
type ProcessorMux struct {
	replToEC *ReplicationToECProcessor
	replGC   *ReplicationGCProcessor
}

// NewProcessorMux constructs a task processor multiplexer.
func NewProcessorMux(replToEC *ReplicationToECProcessor, replGC *ReplicationGCProcessor) *ProcessorMux {
	return &ProcessorMux{
		replToEC: replToEC,
		replGC:   replGC,
	}
}

// ProcessReplicationToEC delegates REPL_TO_EC tasks.
func (m *ProcessorMux) ProcessReplicationToEC(ctx context.Context, task *meta.TieringTask) error {
	if m == nil || m.replToEC == nil {
		return fmt.Errorf("repl-to-ec processor unavailable")
	}
	return m.replToEC.ProcessReplicationToEC(ctx, task)
}

// ProcessReplicationGC delegates GC tasks.
func (m *ProcessorMux) ProcessReplicationGC(ctx context.Context, task *meta.TieringTask) error {
	if m == nil || m.replGC == nil {
		return fmt.Errorf("gc processor unavailable")
	}
	return m.replGC.ProcessReplicationGC(ctx, task)
}
