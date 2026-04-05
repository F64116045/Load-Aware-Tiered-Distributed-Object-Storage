package meta

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type rpcLockEntry struct {
	lock     LeaderLock
	lastBeat time.Time
}

type RPCServer struct {
	repo Repository
	// Optional shared secret checked against X-Meta-Token header.
	authToken string

	lockMu   sync.Mutex
	lockPool map[string]rpcLockEntry
	lockTTL  time.Duration
	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewRPCServer(repo Repository, authToken string) *RPCServer {
	s := &RPCServer{
		repo:      repo,
		authToken: authToken,
		lockPool:  make(map[string]rpcLockEntry),
		lockTTL:   20 * time.Second,
		stopCh:    make(chan struct{}),
	}
	go s.reapStaleLocks()
	return s
}

func (s *RPCServer) Handler() http.Handler {
	return http.HandlerFunc(s.handleRPC)
}

func (s *RPCServer) reapStaleLocks() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.lockMu.Lock()
			now := time.Now()
			for token, entry := range s.lockPool {
				if now.Sub(entry.lastBeat) <= s.lockTTL {
					continue
				}
				_ = entry.lock.Release(context.Background())
				delete(s.lockPool, token)
			}
			s.lockMu.Unlock()
		case <-s.stopCh:
			return
		}
	}
}

func (s *RPCServer) Close() error {
	if s == nil {
		return nil
	}
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})

	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	for token, entry := range s.lockPool {
		_ = entry.lock.Release(context.Background())
		delete(s.lockPool, token)
	}
	return nil
}

func (s *RPCServer) handleRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeRPCError(w, fmt.Errorf("method not allowed"))
		return
	}
	if s.authToken != "" {
		token := r.Header.Get("X-Meta-Token")
		if subtle.ConstantTimeCompare([]byte(token), []byte(s.authToken)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			writeRPCError(w, fmt.Errorf("unauthorized"))
			return
		}
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeRPCError(w, fmt.Errorf("read request failed: %w", err))
		return
	}

	var req rpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeRPCError(w, fmt.Errorf("decode request failed: %w", err))
		return
	}

	result, err := s.dispatch(r.Context(), req)
	if err != nil {
		writeRPCError(w, err)
		return
	}
	writeRPCResult(w, result)
}

func (s *RPCServer) dispatch(ctx context.Context, req rpcRequest) (interface{}, error) {
	switch req.Method {
	case rpcMethodPing:
		return nil, s.repo.Ping(ctx)
	case rpcMethodUpsertNodeHeartbeat:
		var a rpcNodeHeartbeatArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return nil, s.repo.UpsertNodeHeartbeat(ctx, a.NodeID, a.FreeBytes, a.TotalBytes, a.IOQueueDepth, a.CPULoad, a.Status)
	case rpcMethodListHealthyNodeIDs:
		var a rpcListHealthyNodeIDsArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return s.repo.ListHealthyNodeIDs(ctx, a.StaleSec)
	case rpcMethodListNodeHeartbeats:
		var a rpcListNodeHeartbeatsArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return s.repo.ListNodeHeartbeats(ctx, a.Limit)
	case rpcMethodUpsertTieringLeaderState:
		var a rpcLeaderStateUpsertArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return nil, s.repo.UpsertTieringLeaderState(ctx, a.LockKey, a.LeaderID, a.Status)
	case rpcMethodMarkTieringLeaderStopped:
		var a rpcLeaderStateUpsertArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return nil, s.repo.MarkTieringLeaderStopped(ctx, a.LockKey, a.LeaderID, a.Status)
	case rpcMethodGetTieringLeaderState:
		var a rpcLeaderStateGetArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return s.repo.GetTieringLeaderState(ctx, a.LockKey)
	case rpcMethodTryAcquireLeaderLock:
		var a rpcTryAcquireLeaderLockArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		lock, acquired, err := s.repo.TryAcquireLeaderLock(ctx, a.Key)
		if err != nil {
			return nil, err
		}
		if !acquired || lock == nil {
			return rpcTryAcquireLeaderLockResult{Acquired: false}, nil
		}
		token, err := s.newLockToken()
		if err != nil {
			_ = lock.Release(context.Background())
			return nil, err
		}
		s.lockMu.Lock()
		s.lockPool[token] = rpcLockEntry{lock: lock, lastBeat: time.Now()}
		s.lockMu.Unlock()
		return rpcTryAcquireLeaderLockResult{Acquired: true, Token: token}, nil
	case rpcMethodLeaderLockPing:
		var a rpcLeaderLockTokenArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		entry, ok := s.getLockEntry(a.Token)
		if !ok {
			return nil, fmt.Errorf("leader lock token not found")
		}
		if err := entry.lock.Ping(ctx); err != nil {
			return nil, err
		}
		s.touchLockEntry(a.Token)
		return nil, nil
	case rpcMethodLeaderLockRelease:
		var a rpcLeaderLockTokenArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		entry, ok := s.popLockEntry(a.Token)
		if !ok {
			return nil, nil
		}
		return nil, entry.lock.Release(ctx)
	case rpcMethodUpsertNormalizedMetadata:
		var a rpcUpsertNormalizedMetadataArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return nil, s.repo.UpsertNormalizedMetadata(ctx, a.ObjectID, a.Metadata)
	case rpcMethodGetNormalizedMetadata:
		var a rpcObjectIDArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return s.repo.GetNormalizedMetadata(ctx, a.ObjectID)
	case rpcMethodDeleteNormalizedMetadata:
		var a rpcObjectIDArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return nil, s.repo.DeleteNormalizedMetadata(ctx, a.ObjectID)
	case rpcMethodGetObjectAdminView:
		var a rpcObjectIDArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return s.repo.GetObjectAdminView(ctx, a.ObjectID)
	case rpcMethodEnqueueTieringTask:
		var a rpcEnqueueTieringTaskArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return nil, s.repo.EnqueueTieringTask(ctx, a.TaskID, a.ObjectID, a.Version, a.TaskType, a.Priority, a.ScheduledAt)
	case rpcMethodListTieringTasks:
		var a rpcListTieringTasksArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return s.repo.ListTieringTasks(ctx, a.TaskState, a.TaskType, a.Limit)
	case rpcMethodListTieringTaskStateCount:
		var a rpcListTieringTaskStateCountsArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return s.repo.ListTieringTaskStateCounts(ctx, a.TaskType)
	case rpcMethodRequeueTieringTaskNow:
		var a rpcTaskIDArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		v, err := s.repo.RequeueTieringTaskNow(ctx, a.TaskID)
		return rpcBoolResult{Value: v}, err
	case rpcMethodCancelTieringTask:
		var a rpcCancelTieringTaskArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		v, err := s.repo.CancelTieringTask(ctx, a.TaskID, a.Reason)
		return rpcBoolResult{Value: v}, err
	case rpcMethodClaimNextTieringTask:
		var a rpcClaimNextTieringTaskArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return s.repo.ClaimNextTieringTask(ctx, a.TaskType)
	case rpcMethodMarkTieringTaskDone:
		var a rpcTaskIDArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return nil, s.repo.MarkTieringTaskDone(ctx, a.TaskID)
	case rpcMethodMarkTieringTaskRetry:
		var a rpcMarkTieringTaskRetryArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return nil, s.repo.MarkTieringTaskRetry(ctx, a.TaskID, a.LastError, a.NextRunAt)
	case rpcMethodMarkTieringTaskFailed:
		var a rpcMarkTieringTaskFailedArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return nil, s.repo.MarkTieringTaskFailed(ctx, a.TaskID, a.LastError)
	case rpcMethodEnqueueTieringCandidatesA1:
		var a rpcEnqueueTieringCandidatesA1Args
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		v, err := s.repo.EnqueueTieringCandidatesA1(ctx, a.AgeThresholdSec, a.MaxObjects)
		return rpcIntResult{Value: v}, err
	case rpcMethodEnqueueTieringCandidatesA2:
		var a rpcEnqueueTieringCandidatesA2Args
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		v, err := s.repo.EnqueueTieringCandidatesA2(ctx, a.AgeThresholdSec, a.SizeThresholdBytes, a.MaxObjects)
		return rpcIntResult{Value: v}, err
	case rpcMethodEnqueueTieringCandidatesA3:
		var a rpcEnqueueTieringCandidatesA3Args
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		v, err := s.repo.EnqueueTieringCandidatesA3(ctx, a.AgeThresholdSec, a.MaxObjects, a.MaxBytes)
		return rpcIntResult{Value: v}, err
	case rpcMethodEnqueueRepairCandidates:
		var a rpcEnqueueRepairCandidatesArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		v, err := s.repo.EnqueueRepairCandidates(ctx, a.MaxObjects)
		return rpcIntResult{Value: v}, err
	case rpcMethodGetObjectVersionSnapshot:
		var a rpcGetObjectVersionSnapshotArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return s.repo.GetObjectVersionSnapshot(ctx, a.ObjectID, a.TaskVersion)
	case rpcMethodMarkObjectMigrating:
		var a rpcMarkObjectMigratingArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return nil, s.repo.MarkObjectMigrating(ctx, a.ObjectID, a.Version)
	case rpcMethodPromoteObjectVersionToEC:
		var a rpcPromoteObjectVersionToECArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return nil, s.repo.PromoteObjectVersionToEC(ctx, a.ObjectID, a.Version, a.Checksum, a.K, a.M, a.Locations)
	case rpcMethodListActiveReplicaLocations:
		var a rpcListActiveReplicaLocationsArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return s.repo.ListActiveReplicaLocations(ctx, a.ObjectID, a.Version)
	case rpcMethodUpsertReplicaLocations:
		var a rpcUpsertReplicaLocationsArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return nil, s.repo.UpsertReplicaLocations(ctx, a.ObjectID, a.Version, a.NodeIDs)
	case rpcMethodMarkReplicaLocationsDeleted:
		var a rpcMarkReplicaLocationsDeletedArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return nil, s.repo.MarkReplicaLocationsDeleted(ctx, a.ObjectID, a.Version, a.NodeIDs)
	default:
		return nil, fmt.Errorf("unknown rpc method: %s", req.Method)
	}
}

func decodeRPCParams(raw json.RawMessage, out interface{}) error {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode params failed: %w", err)
	}
	return nil
}

func writeRPCResult(w http.ResponseWriter, result interface{}) {
	w.Header().Set("Content-Type", "application/json")
	resp := rpcResponse{OK: true}
	if result != nil {
		raw, err := json.Marshal(result)
		if err != nil {
			writeRPCError(w, fmt.Errorf("encode result failed: %w", err))
			return
		}
		resp.Result = raw
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func writeRPCError(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	resp := rpcResponse{OK: false}
	if err != nil {
		resp.Error = err.Error()
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *RPCServer) newLockToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate lock token failed: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func (s *RPCServer) getLockEntry(token string) (rpcLockEntry, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	entry, ok := s.lockPool[token]
	return entry, ok
}

func (s *RPCServer) touchLockEntry(token string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	entry, ok := s.lockPool[token]
	if !ok {
		return
	}
	entry.lastBeat = time.Now()
	s.lockPool[token] = entry
}

func (s *RPCServer) popLockEntry(token string) (rpcLockEntry, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	entry, ok := s.lockPool[token]
	if !ok {
		return rpcLockEntry{}, false
	}
	delete(s.lockPool, token)
	return entry, true
}
