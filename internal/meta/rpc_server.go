package meta

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type leaderLeaseRepo interface {
	TryAcquireLeaderLease(ctx context.Context, key int64) ([]byte, bool, error)
	RefreshLeaderLease(ctx context.Context, key int64, owner []byte) (bool, error)
	ReleaseLeaderLease(ctx context.Context, key int64, owner []byte) error
}

type RPCServer struct {
	repo Repository
	// Optional shared secret checked against X-Meta-Token header.
	authToken string
}

func NewRPCServer(repo Repository, authToken string) *RPCServer {
	return &RPCServer{
		repo:      repo,
		authToken: authToken,
	}
}

func (s *RPCServer) Handler() http.Handler {
	return http.HandlerFunc(s.handleRPC)
}

func (s *RPCServer) Close() error {
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
		return nil, s.repo.UpsertNodeHeartbeat(ctx, a.NodeID, a.FreeBytes, a.TotalBytes, a.IOQueueDepth, a.CPULoad, a.MemoryUsedPct, a.DiskIOWaitPct, a.Status)
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
		leaseRepo, ok := s.repo.(leaderLeaseRepo)
		if !ok {
			return nil, fmt.Errorf("leader lease backend not supported")
		}
		owner, acquired, err := leaseRepo.TryAcquireLeaderLease(ctx, a.Key)
		if err != nil {
			return nil, err
		}
		if !acquired || len(owner) == 0 {
			return rpcTryAcquireLeaderLockResult{Acquired: false}, nil
		}
		token, err := s.encodeLeaderLockToken(a.Key, owner)
		if err != nil {
			return nil, err
		}
		return rpcTryAcquireLeaderLockResult{Acquired: true, Token: token}, nil
	case rpcMethodLeaderLockPing:
		var a rpcLeaderLockTokenArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		leaseRepo, ok := s.repo.(leaderLeaseRepo)
		if !ok {
			return nil, fmt.Errorf("leader lease backend not supported")
		}
		lockKey, owner, err := s.decodeLeaderLockToken(a.Token)
		if err != nil {
			return nil, err
		}
		stillOwned, err := leaseRepo.RefreshLeaderLease(ctx, lockKey, owner)
		if err != nil {
			return nil, err
		}
		if !stillOwned {
			return nil, fmt.Errorf("leader lock was lost")
		}
		return nil, nil
	case rpcMethodLeaderLockRelease:
		var a rpcLeaderLockTokenArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		leaseRepo, ok := s.repo.(leaderLeaseRepo)
		if !ok {
			return nil, fmt.Errorf("leader lease backend not supported")
		}
		lockKey, owner, err := s.decodeLeaderLockToken(a.Token)
		if err != nil {
			return nil, err
		}
		return nil, leaseRepo.ReleaseLeaderLease(ctx, lockKey, owner)
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
	case rpcMethodGetTieringIndexStats:
		return s.repo.GetTieringIndexStats(ctx)
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
	case rpcMethodEnqueueOldVersionGCCands:
		var a rpcEnqueueOldVersionGCCandidatesArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		v, err := s.repo.EnqueueOldVersionGCCandidates(ctx, a.KeepLatest, a.MinAgeSec, a.MaxTasks)
		return rpcIntResult{Value: v}, err
	case rpcMethodGetObjectVersionSnapshot:
		var a rpcGetObjectVersionSnapshotArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return s.repo.GetObjectVersionSnapshot(ctx, a.ObjectID, a.TaskVersion)
	case rpcMethodGetObjectVersionGCView:
		var a rpcGetObjectVersionGCViewArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return s.repo.GetObjectVersionGCView(ctx, a.ObjectID, a.Version)
	case rpcMethodPurgeObjectVersionMetadata:
		var a rpcPurgeObjectVersionMetadataArgs
		if err := decodeRPCParams(req.Params, &a); err != nil {
			return nil, err
		}
		return nil, s.repo.PurgeObjectVersionMetadata(ctx, a.ObjectID, a.Version)
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

type rpcLeaderTokenPayload struct {
	LockKey int64  `json:"lock_key"`
	Owner   string `json:"owner"`
}

func (s *RPCServer) encodeLeaderLockToken(lockKey int64, owner []byte) (string, error) {
	payload := rpcLeaderTokenPayload{
		LockKey: lockKey,
		Owner:   base64.RawURLEncoding.EncodeToString(owner),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal leader token payload failed: %w", err)
	}
	payloadB64 := base64.RawURLEncoding.EncodeToString(raw)
	sig := s.signLeaderPayload(raw)
	if len(sig) == 0 {
		return "v1." + payloadB64, nil
	}
	return "v1." + payloadB64 + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func (s *RPCServer) decodeLeaderLockToken(token string) (int64, []byte, error) {
	parts := bytes.Split([]byte(token), []byte("."))
	if len(parts) < 2 || string(parts[0]) != "v1" {
		return 0, nil, fmt.Errorf("invalid leader lock token")
	}
	raw, err := base64.RawURLEncoding.DecodeString(string(parts[1]))
	if err != nil {
		return 0, nil, fmt.Errorf("decode leader lock payload failed: %w", err)
	}

	expectSig := s.signLeaderPayload(raw)
	switch {
	case len(expectSig) == 0:
		if len(parts) != 2 {
			return 0, nil, fmt.Errorf("unexpected leader lock token format")
		}
	case len(parts) != 3:
		return 0, nil, fmt.Errorf("invalid signed leader lock token")
	default:
		gotSig, decErr := base64.RawURLEncoding.DecodeString(string(parts[2]))
		if decErr != nil {
			return 0, nil, fmt.Errorf("decode leader lock signature failed: %w", decErr)
		}
		if subtle.ConstantTimeCompare(expectSig, gotSig) != 1 {
			return 0, nil, fmt.Errorf("invalid leader lock token signature")
		}
	}

	var payload rpcLeaderTokenPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, nil, fmt.Errorf("decode leader lock payload json failed: %w", err)
	}
	if payload.LockKey == 0 || payload.Owner == "" {
		return 0, nil, fmt.Errorf("leader lock payload is incomplete")
	}
	owner, err := base64.RawURLEncoding.DecodeString(payload.Owner)
	if err != nil {
		return 0, nil, fmt.Errorf("decode leader lock owner failed: %w", err)
	}
	if len(owner) == 0 {
		return 0, nil, fmt.Errorf("leader lock owner is empty")
	}
	return payload.LockKey, owner, nil
}

func (s *RPCServer) signLeaderPayload(payload []byte) []byte {
	if s.authToken == "" {
		return nil
	}
	mac := hmac.New(sha256.New, []byte(s.authToken))
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}
