package tiering

import (
	"context"
	"fmt"

	"hybrid_distributed_store/internal/meta"
)

// ProcessorMux routes tiering tasks to dedicated processors.
type ProcessorMux struct {
	replToEC   *ReplicationToECProcessor
	replRepair *ReplicationRepairProcessor
	replGC     *ReplicationGCProcessor
}

// NewProcessorMux constructs a task processor multiplexer.
func NewProcessorMux(replToEC *ReplicationToECProcessor, replRepair *ReplicationRepairProcessor, replGC *ReplicationGCProcessor) *ProcessorMux {
	return &ProcessorMux{
		replToEC:   replToEC,
		replRepair: replRepair,
		replGC:     replGC,
	}
}

// ProcessReplicationToEC delegates REPL_TO_EC tasks.
func (m *ProcessorMux) ProcessReplicationToEC(ctx context.Context, task *meta.TieringTask) error {
	if m == nil || m.replToEC == nil {
		return fmt.Errorf("repl-to-ec processor unavailable")
	}
	return m.replToEC.ProcessReplicationToEC(ctx, task)
}

// ProcessReplicationRepair delegates REPAIR tasks.
func (m *ProcessorMux) ProcessReplicationRepair(ctx context.Context, task *meta.TieringTask) error {
	if m == nil || m.replRepair == nil {
		return fmt.Errorf("repair processor unavailable")
	}
	return m.replRepair.ProcessReplicationRepair(ctx, task)
}

// ProcessReplicationGC delegates GC tasks.
func (m *ProcessorMux) ProcessReplicationGC(ctx context.Context, task *meta.TieringTask) error {
	if m == nil || m.replGC == nil {
		return fmt.Errorf("gc processor unavailable")
	}
	return m.replGC.ProcessReplicationGC(ctx, task)
}
