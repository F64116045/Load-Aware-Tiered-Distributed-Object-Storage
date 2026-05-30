package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

type RPCClient struct {
	baseURL    string
	authToken  string
	httpClient *http.Client
}

var _ Repository = (*RPCClient)(nil)

func NewRPCClient(endpoint, authToken string) *RPCClient {
	base := strings.TrimSpace(strings.TrimRight(endpoint, "/"))
	return &RPCClient{
		baseURL:    base,
		authToken:  strings.TrimSpace(authToken),
		httpClient: newRPCHTTPClient(),
	}
}

func newRPCHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          256,
			MaxIdleConnsPerHost:   64,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
}

func (c *RPCClient) rpcURL() string {
	return c.baseURL + "/meta/rpc"
}

func (c *RPCClient) call(ctx context.Context, method string, params interface{}, out interface{}) error {
	if c == nil {
		return fmt.Errorf("rpc client is nil")
	}
	if c.baseURL == "" {
		return fmt.Errorf("rpc client endpoint is empty")
	}

	reqBody := rpcRequest{Method: method}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			return fmt.Errorf("marshal rpc params failed: %w", err)
		}
		reqBody.Params = raw
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal rpc request failed: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.rpcURL(), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build rpc request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.authToken != "" {
		req.Header.Set("X-Meta-Token", c.authToken)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("rpc call failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read rpc response failed: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rpc status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var rpcResp rpcResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return fmt.Errorf("decode rpc response failed: %w", err)
	}
	if !rpcResp.OK {
		if rpcResp.Error == "" {
			rpcResp.Error = "unknown rpc error"
		}
		return fmt.Errorf("rpc %s failed: %s", method, rpcResp.Error)
	}
	if out != nil && len(rpcResp.Result) > 0 {
		if err := json.Unmarshal(rpcResp.Result, out); err != nil {
			return fmt.Errorf("decode rpc result failed: %w", err)
		}
	}
	return nil
}

func (c *RPCClient) Ping(ctx context.Context) error {
	return c.call(ctx, rpcMethodPing, nil, nil)
}

func (c *RPCClient) Close() error {
	return nil
}

func (c *RPCClient) UpsertNodeHeartbeat(ctx context.Context, nodeID string, freeBytes int64, totalBytes int64, ioQueueDepth int, ioQueueBytes int64, cpuLoad float64, memoryUsedPct float64, diskIOWaitPct float64, status string) error {
	return c.call(ctx, rpcMethodUpsertNodeHeartbeat, rpcNodeHeartbeatArgs{
		NodeID:        nodeID,
		FreeBytes:     freeBytes,
		TotalBytes:    totalBytes,
		IOQueueDepth:  ioQueueDepth,
		IOQueueBytes:  ioQueueBytes,
		CPULoad:       cpuLoad,
		MemoryUsedPct: memoryUsedPct,
		DiskIOWaitPct: diskIOWaitPct,
		Status:        status,
	}, nil)
}

func (c *RPCClient) ListHealthyNodeIDs(ctx context.Context, staleSec int) ([]string, error) {
	var out []string
	err := c.call(ctx, rpcMethodListHealthyNodeIDs, rpcListHealthyNodeIDsArgs{StaleSec: staleSec}, &out)
	return out, err
}

func (c *RPCClient) ListNodeHeartbeats(ctx context.Context, limit int) ([]NodeHeartbeatSnapshot, error) {
	var out []NodeHeartbeatSnapshot
	err := c.call(ctx, rpcMethodListNodeHeartbeats, rpcListNodeHeartbeatsArgs{Limit: limit}, &out)
	return out, err
}

func (c *RPCClient) UpsertTieringLeaderState(ctx context.Context, lockKey int64, leaderID, status string) error {
	return c.call(ctx, rpcMethodUpsertTieringLeaderState, rpcLeaderStateUpsertArgs{
		LockKey:  lockKey,
		LeaderID: leaderID,
		Status:   status,
	}, nil)
}

func (c *RPCClient) MarkTieringLeaderStopped(ctx context.Context, lockKey int64, leaderID, status string) error {
	return c.call(ctx, rpcMethodMarkTieringLeaderStopped, rpcLeaderStateUpsertArgs{
		LockKey:  lockKey,
		LeaderID: leaderID,
		Status:   status,
	}, nil)
}

func (c *RPCClient) GetTieringLeaderState(ctx context.Context, lockKey int64) (*TieringLeaderState, error) {
	var out *TieringLeaderState
	err := c.call(ctx, rpcMethodGetTieringLeaderState, rpcLeaderStateGetArgs{LockKey: lockKey}, &out)
	return out, err
}

func (c *RPCClient) TryAcquireLeaderLock(ctx context.Context, key int64) (LeaderLock, bool, error) {
	var out rpcTryAcquireLeaderLockResult
	if err := c.call(ctx, rpcMethodTryAcquireLeaderLock, rpcTryAcquireLeaderLockArgs{Key: key}, &out); err != nil {
		return nil, false, err
	}
	if !out.Acquired || out.Token == "" {
		return nil, false, nil
	}
	return &rpcLeaderLock{
		client: c,
		token:  out.Token,
	}, true, nil
}

type rpcLeaderLock struct {
	client *RPCClient
	token  string
}

func (l *rpcLeaderLock) Ping(ctx context.Context) error {
	if l == nil || l.client == nil || l.token == "" {
		return fmt.Errorf("rpc leader lock is not active")
	}
	return l.client.call(ctx, rpcMethodLeaderLockPing, rpcLeaderLockTokenArgs{Token: l.token}, nil)
}

func (l *rpcLeaderLock) Release(ctx context.Context) error {
	if l == nil || l.client == nil || l.token == "" {
		return nil
	}
	token := l.token
	l.token = ""
	return l.client.call(ctx, rpcMethodLeaderLockRelease, rpcLeaderLockTokenArgs{Token: token}, nil)
}

func (c *RPCClient) UpsertNormalizedMetadata(ctx context.Context, objectID string, metadata map[string]interface{}) error {
	return c.call(ctx, rpcMethodUpsertNormalizedMetadata, rpcUpsertNormalizedMetadataArgs{
		ObjectID: objectID,
		Metadata: metadata,
	}, nil)
}

func (c *RPCClient) GetNormalizedMetadata(ctx context.Context, objectID string) (map[string]interface{}, error) {
	var out map[string]interface{}
	err := c.call(ctx, rpcMethodGetNormalizedMetadata, rpcObjectIDArgs{ObjectID: objectID}, &out)
	return out, err
}

func (c *RPCClient) DeleteNormalizedMetadata(ctx context.Context, objectID string) error {
	return c.call(ctx, rpcMethodDeleteNormalizedMetadata, rpcObjectIDArgs{ObjectID: objectID}, nil)
}

func (c *RPCClient) GetObjectAdminView(ctx context.Context, objectID string) (*ObjectAdminView, error) {
	var out *ObjectAdminView
	err := c.call(ctx, rpcMethodGetObjectAdminView, rpcObjectIDArgs{ObjectID: objectID}, &out)
	return out, err
}

func (c *RPCClient) GetTieringIndexStats(ctx context.Context) (*TieringIndexStats, error) {
	var out *TieringIndexStats
	err := c.call(ctx, rpcMethodGetTieringIndexStats, nil, &out)
	return out, err
}

func (c *RPCClient) EnqueueTieringTask(ctx context.Context, taskID, objectID string, version int64, taskType string, priority int, scheduledAt time.Time) error {
	return c.call(ctx, rpcMethodEnqueueTieringTask, rpcEnqueueTieringTaskArgs{
		TaskID:      taskID,
		ObjectID:    objectID,
		Version:     version,
		TaskType:    taskType,
		Priority:    priority,
		ScheduledAt: scheduledAt,
	}, nil)
}

func (c *RPCClient) ListTieringTasks(ctx context.Context, taskState, taskType string, limit int) ([]TieringTask, error) {
	var out []TieringTask
	err := c.call(ctx, rpcMethodListTieringTasks, rpcListTieringTasksArgs{
		TaskState: taskState,
		TaskType:  taskType,
		Limit:     limit,
	}, &out)
	return out, err
}

func (c *RPCClient) ListTieringTaskStateCounts(ctx context.Context, taskType string) (map[string]int64, error) {
	var out map[string]int64
	err := c.call(ctx, rpcMethodListTieringTaskStateCount, rpcListTieringTaskStateCountsArgs{TaskType: taskType}, &out)
	return out, err
}

func (c *RPCClient) RequeueTieringTaskNow(ctx context.Context, taskID string) (bool, error) {
	var out rpcBoolResult
	err := c.call(ctx, rpcMethodRequeueTieringTaskNow, rpcTaskIDArgs{TaskID: taskID}, &out)
	return out.Value, err
}

func (c *RPCClient) CancelTieringTask(ctx context.Context, taskID, reason string) (bool, error) {
	var out rpcBoolResult
	err := c.call(ctx, rpcMethodCancelTieringTask, rpcCancelTieringTaskArgs{TaskID: taskID, Reason: reason}, &out)
	return out.Value, err
}

func (c *RPCClient) ClaimNextTieringTask(ctx context.Context, taskType string) (*TieringTask, error) {
	var out *TieringTask
	err := c.call(ctx, rpcMethodClaimNextTieringTask, rpcClaimNextTieringTaskArgs{TaskType: taskType}, &out)
	return out, err
}

func (c *RPCClient) MarkTieringTaskDone(ctx context.Context, taskID string) error {
	return c.call(ctx, rpcMethodMarkTieringTaskDone, rpcTaskIDArgs{TaskID: taskID}, nil)
}

func (c *RPCClient) MarkTieringTaskRetry(ctx context.Context, taskID, lastErr string, nextRunAt time.Time) error {
	return c.call(ctx, rpcMethodMarkTieringTaskRetry, rpcMarkTieringTaskRetryArgs{
		TaskID:    taskID,
		LastError: lastErr,
		NextRunAt: nextRunAt,
	}, nil)
}

func (c *RPCClient) MarkTieringTaskFailed(ctx context.Context, taskID, lastErr string) error {
	return c.call(ctx, rpcMethodMarkTieringTaskFailed, rpcMarkTieringTaskFailedArgs{
		TaskID:    taskID,
		LastError: lastErr,
	}, nil)
}

func (c *RPCClient) PurgeTerminalTieringTasks(ctx context.Context, olderThan time.Time, limit int) (int, error) {
	var out rpcIntResult
	err := c.call(ctx, rpcMethodPurgeTerminalTieringTasks, rpcPurgeTerminalTieringTasksArgs{
		OlderThan: olderThan,
		Limit:     limit,
	}, &out)
	return out.Value, err
}

func (c *RPCClient) EnqueueTieringCandidatesStrategyA(ctx context.Context, ageThresholdSec int, maxObjects int) (int, error) {
	var out rpcIntResult
	err := c.call(ctx, rpcMethodEnqueueTieringCandidatesA, rpcEnqueueTieringCandidatesAArgs{
		AgeThresholdSec: ageThresholdSec,
		MaxObjects:      maxObjects,
	}, &out)
	return out.Value, err
}

func (c *RPCClient) EnqueueTieringCandidatesStrategyB(ctx context.Context, ageThresholdSec int, maxObjects int, maxBytes int64) (int, error) {
	var out rpcIntResult
	err := c.call(ctx, rpcMethodEnqueueTieringCandidatesB, rpcEnqueueTieringCandidatesBArgs{
		AgeThresholdSec: ageThresholdSec,
		MaxObjects:      maxObjects,
		MaxBytes:        maxBytes,
	}, &out)
	return out.Value, err
}

func (c *RPCClient) EnqueueTieringCandidatesStrategyC(ctx context.Context, ageThresholdSec int, maxObjects int, maxBytes int64) (int, error) {
	var out rpcIntResult
	err := c.call(ctx, rpcMethodEnqueueTieringCandidatesC, rpcEnqueueTieringCandidatesCArgs{
		AgeThresholdSec: ageThresholdSec,
		MaxObjects:      maxObjects,
		MaxBytes:        maxBytes,
	}, &out)
	return out.Value, err
}

func (c *RPCClient) EnqueueRepairCandidates(ctx context.Context, maxObjects int) (int, error) {
	var out rpcIntResult
	err := c.call(ctx, rpcMethodEnqueueRepairCandidates, rpcEnqueueRepairCandidatesArgs{
		MaxObjects: maxObjects,
	}, &out)
	return out.Value, err
}

func (c *RPCClient) EnqueueOldVersionGCCandidates(ctx context.Context, keepLatest int, minAgeSec int, maxTasks int) (int, error) {
	var out rpcIntResult
	err := c.call(ctx, rpcMethodEnqueueOldVersionGCCands, rpcEnqueueOldVersionGCCandidatesArgs{
		KeepLatest: keepLatest,
		MinAgeSec:  minAgeSec,
		MaxTasks:   maxTasks,
	}, &out)
	return out.Value, err
}

func (c *RPCClient) GetObjectVersionSnapshot(ctx context.Context, objectID string, taskVersion int64) (*ObjectVersionSnapshot, error) {
	var out *ObjectVersionSnapshot
	err := c.call(ctx, rpcMethodGetObjectVersionSnapshot, rpcGetObjectVersionSnapshotArgs{
		ObjectID:    objectID,
		TaskVersion: taskVersion,
	}, &out)
	return out, err
}

func (c *RPCClient) GetObjectVersionGCView(ctx context.Context, objectID string, version int64) (*ObjectVersionGCView, error) {
	var out *ObjectVersionGCView
	err := c.call(ctx, rpcMethodGetObjectVersionGCView, rpcGetObjectVersionGCViewArgs{
		ObjectID: objectID,
		Version:  version,
	}, &out)
	return out, err
}

func (c *RPCClient) PurgeObjectVersionMetadata(ctx context.Context, objectID string, version int64) error {
	return c.call(ctx, rpcMethodPurgeObjectVersionMetadata, rpcPurgeObjectVersionMetadataArgs{
		ObjectID: objectID,
		Version:  version,
	}, nil)
}

func (c *RPCClient) MarkObjectMigrating(ctx context.Context, objectID string, version int64) error {
	return c.call(ctx, rpcMethodMarkObjectMigrating, rpcMarkObjectMigratingArgs{
		ObjectID: objectID,
		Version:  version,
	}, nil)
}

func (c *RPCClient) PromoteObjectVersionToEC(ctx context.Context, objectID string, version int64, checksum string, k int, m int, locations []ECShardLocation) error {
	return c.call(ctx, rpcMethodPromoteObjectVersionToEC, rpcPromoteObjectVersionToECArgs{
		ObjectID:  objectID,
		Version:   version,
		Checksum:  checksum,
		K:         k,
		M:         m,
		Locations: locations,
	}, nil)
}

func (c *RPCClient) ListActiveReplicaLocations(ctx context.Context, objectID string, version int64) ([]ReplicaLocation, error) {
	var out []ReplicaLocation
	err := c.call(ctx, rpcMethodListActiveReplicaLocations, rpcListActiveReplicaLocationsArgs{
		ObjectID: objectID,
		Version:  version,
	}, &out)
	return out, err
}

func (c *RPCClient) UpsertReplicaLocations(ctx context.Context, objectID string, version int64, nodeIDs []string) error {
	return c.call(ctx, rpcMethodUpsertReplicaLocations, rpcUpsertReplicaLocationsArgs{
		ObjectID: objectID,
		Version:  version,
		NodeIDs:  nodeIDs,
	}, nil)
}

func (c *RPCClient) MarkReplicaLocationsDeleted(ctx context.Context, objectID string, version int64, nodeIDs []string) error {
	return c.call(ctx, rpcMethodMarkReplicaLocationsDeleted, rpcMarkReplicaLocationsDeletedArgs{
		ObjectID: objectID,
		Version:  version,
		NodeIDs:  nodeIDs,
	}, nil)
}
