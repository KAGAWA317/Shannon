package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/streaming"
	"github.com/go-redis/redis/v8"
)

// MessageType indicates a simple protocol for P2P
// MessageType 表示 P2P 使用的简单协议。
type MessageType string

const (
	MessageTypeRequest    MessageType = "request"
	MessageTypeOffer      MessageType = "offer"
	MessageTypeAccept     MessageType = "accept"
	MessageTypeDelegation MessageType = "delegation"
	MessageTypeInfo       MessageType = "info"
)

type SendAgentMessageInput struct {
	WorkflowID string         `json:"workflow_id"`
	From       string         `json:"from"`
	To         string         `json:"to"`
	Type       MessageType    `json:"type"`
	Payload    map[string]any `json:"payload"`
	Timestamp  time.Time      `json:"timestamp"` // Passed from workflow.Now()
	// 由 workflow.Now() 传入。
}

type SendAgentMessageResult struct {
	Seq uint64 `json:"seq"`
}

// SendAgentMessage stores a message under deterministic Redis keys and publishes a streaming event
// SendAgentMessage 把消息写入确定性的 Redis key 下，并发布 streaming event。
func (a *Activities) SendAgentMessage(ctx context.Context, in SendAgentMessageInput) (SendAgentMessageResult, error) {
	if in.WorkflowID == "" || in.To == "" || in.From == "" {
		return SendAgentMessageResult{}, fmt.Errorf("invalid message args")
	}

	// Validate payload size to prevent memory exhaustion
	// 校验 payload 大小，防止内存耗尽。
	const maxPayloadSize = 1 * 1024 * 1024 // 1MB limit
	// 上限 1MB。
	payloadSize := lenMust(in.Payload)
	if payloadSize > maxPayloadSize {
		return SendAgentMessageResult{}, fmt.Errorf("payload size %d exceeds maximum allowed size of %d bytes", payloadSize, maxPayloadSize)
	}

	// Policy gate via existing team action authorizer
	// 复用既有的 team action authorizer 做 policy 校验。
	_, _ = AuthorizeTeamAction(ctx, TeamActionInput{Action: "message_send", SessionID: "", UserID: "", AgentID: in.From, Role: "", Metadata: map[string]any{
		"to": in.To, "type": string(in.Type), "size": payloadSize,
	}})

	rc := a.sessionManager.RedisWrapper().GetClient()
	seqKey := fmt.Sprintf("wf:%s:mbox:%s:seq", in.WorkflowID, in.To)
	listKey := fmt.Sprintf("wf:%s:mbox:%s:msgs", in.WorkflowID, in.To)
	seq := rc.Incr(ctx, seqKey).Val()
	// Use timestamp from workflow for deterministic replay
	// 使用 workflow 传入的 timestamp，保证 replay 确定性。
	ts := in.Timestamp
	if ts.IsZero() {
		ts = time.Now() // Fallback for backward compatibility
		// 向后兼容的兜底。
	}
	msg := map[string]any{
		"seq":     seq,
		"from":    in.From,
		"to":      in.To,
		"type":    string(in.Type),
		"payload": in.Payload,
		"ts":      ts.UnixNano(),
	}
	b, _ := json.Marshal(msg)
	if err := rc.RPush(ctx, listKey, b).Err(); err != nil {
		return SendAgentMessageResult{}, err
	}
	// Set TTL for resource cleanup (48 hours)
	// 设置 TTL 便于资源回收（48 小时）。
	rc.Expire(ctx, seqKey, 48*time.Hour)
	rc.Expire(ctx, listKey, 48*time.Hour)

	// Publish streaming events using workflow timestamp
	// 用 workflow timestamp 发布 streaming event。
	evt := streaming.Event{WorkflowID: in.WorkflowID, Type: string(StreamEventMessageSent), AgentID: in.From, Message: "", Timestamp: ts, Seq: 0}
	streaming.Get().Publish(in.WorkflowID, evt)
	// Receiver event (for dashboards)
	// 接收方 event（供 dashboard 使用）。
	evtR := streaming.Event{WorkflowID: in.WorkflowID, Type: string(StreamEventMessageReceived), AgentID: in.To, Message: "", Timestamp: ts, Seq: 0}
	streaming.Get().Publish(in.WorkflowID, evtR)

	return SendAgentMessageResult{Seq: uint64(seq)}, nil
}

type FetchAgentMessagesInput struct {
	WorkflowID string `json:"workflow_id"`
	AgentID    string `json:"agent_id"`
	SinceSeq   uint64 `json:"since_seq"`
	Limit      int64  `json:"limit"`
}

type AgentMessage struct {
	Seq     uint64         `json:"seq"`
	From    string         `json:"from"`
	To      string         `json:"to"`
	Type    MessageType    `json:"type"`
	Payload map[string]any `json:"payload"`
	Ts      int64          `json:"ts"`
}

// FetchAgentMessages returns messages for an agent after SinceSeq (best-effort)
// FetchAgentMessages 返回某 agent 在 SinceSeq 之后的消息（best-effort）。
func (a *Activities) FetchAgentMessages(ctx context.Context, in FetchAgentMessagesInput) ([]AgentMessage, error) {
	if in.WorkflowID == "" || in.AgentID == "" {
		return nil, fmt.Errorf("invalid args")
	}
	rc := a.sessionManager.RedisWrapper().GetClient()
	listKey := fmt.Sprintf("wf:%s:mbox:%s:msgs", in.WorkflowID, in.AgentID)
	// Fetch recent N items; simple window to avoid huge scans
	// 只取最近 N 条；用简单窗口避免超大 scan。
	if in.Limit <= 0 {
		in.Limit = 200
	}
	// Get list length and compute range
	// 取 list 长度并计算读取范围。
	llen := rc.LLen(ctx, listKey).Val()
	start := max(llen-in.Limit, 0)
	vals, err := rc.LRange(ctx, listKey, start, llen).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	out := make([]AgentMessage, 0, len(vals))
	for _, v := range vals {
		var m AgentMessage
		if json.Unmarshal([]byte(v), &m) == nil {
			if m.Seq > in.SinceSeq {
				out = append(out, m)
			}
		}
	}
	return out, nil
}

type WorkspaceAppendInput struct {
	WorkflowID string         `json:"workflow_id"`
	Topic      string         `json:"topic"`
	Entry      map[string]any `json:"entry"`
	Timestamp  time.Time      `json:"timestamp"` // Passed from workflow.Now()
	// 由 workflow.Now() 传入。
}

type WorkspaceAppendResult struct {
	Seq uint64 `json:"seq"`
}

// WorkspaceAppend appends an entry to a topic list with global workspace seq
// WorkspaceAppend 把 entry 追加到 topic list，并使用全局 workspace seq。
func (a *Activities) WorkspaceAppend(ctx context.Context, in WorkspaceAppendInput) (WorkspaceAppendResult, error) {
	if in.WorkflowID == "" || in.Topic == "" {
		return WorkspaceAppendResult{}, fmt.Errorf("invalid args")
	}

	// Validate entry size to prevent memory exhaustion
	// 校验 entry 大小，防止内存耗尽。
	const maxEntrySize = 1 * 1024 * 1024 // 1MB limit
	// 上限 1MB。
	entrySize := lenMust(in.Entry)
	if entrySize > maxEntrySize {
		return WorkspaceAppendResult{}, fmt.Errorf("entry size %d exceeds maximum allowed size of %d bytes", entrySize, maxEntrySize)
	}

	// Policy gate
	// Policy 校验。
	_, _ = AuthorizeTeamAction(ctx, TeamActionInput{Action: "workspace_append", Metadata: map[string]any{"topic": in.Topic, "size": entrySize}})
	rc := a.sessionManager.RedisWrapper().GetClient()
	seqKey := fmt.Sprintf("wf:%s:ws:seq", in.WorkflowID)
	seq := rc.Incr(ctx, seqKey).Val()
	listKey := fmt.Sprintf("wf:%s:ws:%s", in.WorkflowID, in.Topic)
	// Use timestamp from workflow for deterministic replay
	// 使用 workflow 传入的 timestamp，保证 replay 确定性。
	ts := in.Timestamp
	if ts.IsZero() {
		ts = time.Now() // Fallback for backward compatibility
		// 向后兼容的兜底。
	}
	entry := map[string]any{"seq": seq, "topic": in.Topic, "entry": in.Entry, "ts": ts.UnixNano()}
	b, _ := json.Marshal(entry)
	if err := rc.RPush(ctx, listKey, b).Err(); err != nil {
		return WorkspaceAppendResult{}, err
	}
	// Set TTL for resource cleanup (48 hours)
	// 设置 TTL 便于资源回收（48 小时）。
	rc.Expire(ctx, seqKey, 48*time.Hour)
	rc.Expire(ctx, listKey, 48*time.Hour)
	// stream event
	// 发布 stream event。
	streaming.Get().Publish(in.WorkflowID, streaming.Event{WorkflowID: in.WorkflowID, Type: string(StreamEventWorkspaceUpdated), AgentID: "workspace", Message: "", Timestamp: ts})
	return WorkspaceAppendResult{Seq: uint64(seq)}, nil
}

type WorkspaceListInput struct {
	WorkflowID string `json:"workflow_id"`
	Topic      string `json:"topic"`
	SinceSeq   uint64 `json:"since_seq"`
	Limit      int64  `json:"limit"`
}

type WorkspaceEntry struct {
	Seq   uint64         `json:"seq"`
	Topic string         `json:"topic"`
	Entry map[string]any `json:"entry"`
	Ts    int64          `json:"ts"`
}

// WorkspaceList returns entries for a topic after SinceSeq
// WorkspaceList 返回某 topic 在 SinceSeq 之后的 entry。
func (a *Activities) WorkspaceList(ctx context.Context, in WorkspaceListInput) ([]WorkspaceEntry, error) {
	if in.WorkflowID == "" || in.Topic == "" {
		return nil, fmt.Errorf("invalid args")
	}
	rc := a.sessionManager.RedisWrapper().GetClient()
	listKey := fmt.Sprintf("wf:%s:ws:%s", in.WorkflowID, in.Topic)
	if in.Limit <= 0 {
		in.Limit = 200
	}
	llen := rc.LLen(ctx, listKey).Val()
	start := max(llen-in.Limit, 0)
	vals, err := rc.LRange(ctx, listKey, start, llen).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	out := make([]WorkspaceEntry, 0, len(vals))
	for _, v := range vals {
		var e WorkspaceEntry
		if json.Unmarshal([]byte(v), &e) == nil {
			if e.Seq > in.SinceSeq {
				out = append(out, e)
			}
		}
	}
	return out, nil
}

// WorkspaceListAllInput scans all workspace topics for a workflow.
// WorkspaceListAllInput 用于扫描某个 workflow 的全部 workspace topic。
type WorkspaceListAllInput struct {
	WorkflowID string `json:"workflow_id"`
	SinceSeq   uint64 `json:"since_seq"`
	MaxEntries int    `json:"max_entries"` // Max entries total across all topics
	// 所有 topic 合计的最大 entry 数。
}

// WorkspaceListAll returns recent entries from ALL workspace topics for a workflow.
// WorkspaceListAll 返回某 workflow 全部 workspace topic 的近期 entry。
func (a *Activities) WorkspaceListAll(ctx context.Context, in WorkspaceListAllInput) ([]WorkspaceEntry, error) {
	if in.WorkflowID == "" {
		return nil, fmt.Errorf("invalid args: empty workflow_id")
	}
	if in.MaxEntries <= 0 {
		in.MaxEntries = 10
	}

	rc := a.sessionManager.RedisWrapper().GetClient()
	prefix := fmt.Sprintf("wf:%s:ws:", in.WorkflowID)
	seqKey := fmt.Sprintf("wf:%s:ws:seq", in.WorkflowID)

	// Scan for workspace topic keys (capped to prevent memory bloat)
	// 扫描 workspace topic 的 key（设上限，避免内存膨胀）。
	const maxTopics = 20
	var topicKeys []string
	iter := rc.Scan(ctx, 0, prefix+"*", 100).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		if key != seqKey { // Skip the global seq counter
			// 跳过全局 seq 计数器。
			topicKeys = append(topicKeys, key)
			if len(topicKeys) >= maxTopics {
				break
			}
		}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	// Read recent entries from each topic
	// 从每个 topic 读取近期 entry。
	var out []WorkspaceEntry
	for _, key := range topicKeys {
		llen := rc.LLen(ctx, key).Val()
		start := max(
			// Last 5 per topic
			// 每个 topic 取最后 5 条。
			llen-5, 0)
		vals, err := rc.LRange(ctx, key, start, llen).Result()
		if err != nil && err != redis.Nil {
			continue
		}
		for _, v := range vals {
			var e WorkspaceEntry
			if json.Unmarshal([]byte(v), &e) == nil {
				if e.Seq > in.SinceSeq {
					out = append(out, e)
				}
			}
		}
	}

	// Sort by seq (entries come from multiple Redis lists, so order isn't guaranteed)
	// 按 seq 排序（entry 来自多个 Redis list，顺序没有保证）。
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })

	// Cap at MaxEntries, keeping the most recent (highest seq)
	// 截到 MaxEntries，保留最新的（seq 最大）。
	if len(out) > in.MaxEntries {
		out = out[len(out)-in.MaxEntries:]
	}

	return out, nil
}

// ── File Registry Activities ────────────────────────────────────────────────

// FileRegistryEntry represents a file tracked in the swarm file registry.
// FileRegistryEntry 表示 swarm file registry 中跟踪的一个文件。
type FileRegistryEntry struct {
	Path      string `json:"path"`
	Author    string `json:"author"`
	Size      int    `json:"size"`
	Summary   string `json:"summary"`
	CreatedAt string `json:"created_at"`
}

// RegisterFileInput is the input for RegisterFile activity.
// RegisterFileInput 是 RegisterFile activity 的输入。
type RegisterFileInput struct {
	WorkflowID string `json:"workflow_id"`
	Path       string `json:"path"`
	Author     string `json:"author"`
	Size       int    `json:"size"`
	Summary    string `json:"summary"`
}

// RegisterFile records a file entry in the Redis-backed file registry.
// RegisterFile 把一条文件记录写入 Redis-backed 的 file registry。
func (a *Activities) RegisterFile(ctx context.Context, in RegisterFileInput) error {
	if in.WorkflowID == "" || in.Path == "" {
		return fmt.Errorf("invalid args: empty workflow_id or path")
	}
	rc := a.sessionManager.RedisWrapper().GetClient()
	key := fmt.Sprintf("wf:%s:file_registry", in.WorkflowID)
	entry := FileRegistryEntry{
		Path:      in.Path,
		Author:    in.Author,
		Size:      in.Size,
		Summary:   in.Summary,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal file registry entry: %w", err)
	}
	if err := rc.HSet(ctx, key, in.Path, string(data)).Err(); err != nil {
		return err
	}
	rc.Expire(ctx, key, 24*time.Hour)
	return nil
}

// GetFileRegistryInput is the input for GetFileRegistry activity.
// GetFileRegistryInput 是 GetFileRegistry activity 的输入。
type GetFileRegistryInput struct {
	WorkflowID string `json:"workflow_id"`
}

// GetFileRegistry returns all file entries from the Redis-backed file registry, sorted by path.
// GetFileRegistry 返回 Redis-backed file registry 的全部文件记录，按 path 排序。
func (a *Activities) GetFileRegistry(ctx context.Context, in GetFileRegistryInput) ([]FileRegistryEntry, error) {
	if in.WorkflowID == "" {
		return nil, fmt.Errorf("invalid args: empty workflow_id")
	}
	rc := a.sessionManager.RedisWrapper().GetClient()
	key := fmt.Sprintf("wf:%s:file_registry", in.WorkflowID)
	vals, err := rc.HGetAll(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	out := make([]FileRegistryEntry, 0, len(vals))
	for _, v := range vals {
		var e FileRegistryEntry
		if json.Unmarshal([]byte(v), &e) == nil {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

// lenMust returns an approximate JSON size of a payload.
// Returns math.MaxInt on marshal failure so the size check never passes silently.
// lenMust 返回 payload 的近似 JSON 大小。
// 序列化失败时返回 math.MaxInt，确保大小校验不会静默通过。
func lenMust(m map[string]any) int {
	b, err := json.Marshal(m)
	if err != nil {
		return math.MaxInt
	}
	return len(b)
}

// Structured protocols (v1)
type TaskRequest struct {
	TaskID      string   `json:"task_id"`
	Description string   `json:"description"`
	RequiredBy  int64    `json:"required_by,omitempty"`
	Skills      []string `json:"skills,omitempty"`
	Topic       string   `json:"topic,omitempty"`
}

type TaskOffer struct {
	RequestID     string  `json:"request_id"`
	AgentID       string  `json:"agent_id"`
	Confidence    float64 `json:"confidence,omitempty"`
	EstimateHours int     `json:"estimate_hours,omitempty"`
}

type TaskAccept struct {
	RequestID string `json:"request_id"`
	AgentID   string `json:"agent_id"`
}

// Convenience wrappers
func (a *Activities) SendTaskRequest(ctx context.Context, wf, from, to string, req TaskRequest, timestamp time.Time) (SendAgentMessageResult, error) {
	payload := map[string]any{
		"task_id": req.TaskID, "description": req.Description, "required_by": req.RequiredBy, "skills": req.Skills, "topic": req.Topic,
	}
	return a.SendAgentMessage(ctx, SendAgentMessageInput{WorkflowID: wf, From: from, To: to, Type: MessageTypeRequest, Payload: payload, Timestamp: timestamp})
}

func (a *Activities) SendTaskOffer(ctx context.Context, wf, from, to string, off TaskOffer, timestamp time.Time) (SendAgentMessageResult, error) {
	payload := map[string]any{
		"request_id": off.RequestID, "agent_id": off.AgentID, "confidence": off.Confidence, "estimate_hours": off.EstimateHours,
	}
	return a.SendAgentMessage(ctx, SendAgentMessageInput{WorkflowID: wf, From: from, To: to, Type: MessageTypeOffer, Payload: payload, Timestamp: timestamp})
}

func (a *Activities) SendTaskAccept(ctx context.Context, wf, from, to string, ac TaskAccept, timestamp time.Time) (SendAgentMessageResult, error) {
	payload := map[string]any{"request_id": ac.RequestID, "agent_id": ac.AgentID}
	return a.SendAgentMessage(ctx, SendAgentMessageInput{WorkflowID: wf, From: from, To: to, Type: MessageTypeAccept, Payload: payload, Timestamp: timestamp})
}

// validateSessionID checks session_id to prevent path traversal attacks.
// Mirrors Python _validate_session_id() in file_ops.py.
// validateSessionID 校验 session_id，防止路径穿越攻击。
// 与 Python file_ops.py 中的 _validate_session_id() 保持一致。
func validateSessionID(sessionID string) error {
	if sessionID == "" {
		return fmt.Errorf("empty session_id")
	}
	if len(sessionID) > 128 {
		return fmt.Errorf("session_id too long (max 128 chars)")
	}
	if strings.Contains(sessionID, "..") || strings.HasPrefix(sessionID, ".") {
		return fmt.Errorf("session_id contains path traversal")
	}
	for _, c := range sessionID {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.') {
			return fmt.Errorf("session_id contains invalid char: %c", c)
		}
	}
	return nil
}

// SetupWorkspaceDirsInput is the input for the SetupWorkspaceDirs activity.
// SetupWorkspaceDirsInput 是 SetupWorkspaceDirs activity 的输入。
type SetupWorkspaceDirsInput struct {
	WorkflowID string `json:"workflow_id"`
	SessionID  string `json:"session_id"`
}

// SetupWorkspaceDirs ensures the session workspace root directory exists.
// Subdirectories are created on-demand by file_write (create_dirs=true).
// SetupWorkspaceDirs 确保 session workspace 根目录存在。
// 子目录由 file_write 按需创建（create_dirs=true）。
func (a *Activities) SetupWorkspaceDirs(ctx context.Context, in SetupWorkspaceDirsInput) error {
	if err := validateSessionID(in.SessionID); err != nil {
		return fmt.Errorf("invalid session_id: %w", err)
	}
	baseDir := getenv("SHANNON_SESSION_WORKSPACES_DIR", "/tmp/shannon-sessions")
	sessionDir := filepath.Join(baseDir, in.SessionID)
	if err := os.MkdirAll(sessionDir, 0755); err != nil {
		return fmt.Errorf("failed to create workspace dir: %w", err)
	}
	return nil
}

// ── TaskList Activities ─────────────────────────────────────────────────────

// SwarmTask represents a single task in the Redis-backed TaskList.
// SwarmTask 表示 Redis-backed TaskList 中的单个 task。
type SwarmTask struct {
	ID          string `json:"id"`
	Description string `json:"description"`
	Status      string `json:"status"` // "pending", "in_progress", "completed"
	// 取值："pending"、"in_progress"、"completed"。
	Owner string `json:"owner"` // agent ID that owns this task
	// 拥有该 task 的 agent ID。
	CreatedBy string `json:"created_by"` // "decompose" or agent ID
	// "decompose" 或 agent ID。
	DependsOn []string `json:"depends_on"` // task IDs this depends on
	// 该 task 依赖的 task ID。
	CreatedAt   string `json:"created_at"`
	CompletedAt string `json:"completed_at,omitempty"`
}

// taskListKey returns the Redis hash key for a workflow's task list.
// taskListKey 返回某个 workflow task list 的 Redis hash key。
func taskListKey(workflowID string) string {
	return fmt.Sprintf("wf:%s:tasklist", workflowID)
}

// publishTaskListUpdate publishes TASKLIST_UPDATED with full task payload.
// tasks is fetched from Redis by the caller (ctx-aware) to avoid context mismatch.
// publishTaskListUpdate 发布带完整 task payload 的 TASKLIST_UPDATED。
// tasks 由调用方从 Redis 读取（带 ctx），避免 context 不一致。
func publishTaskListUpdate(workflowID, agentID, message string, tasks []SwarmTask) {
	// Convert to generic maps for JSON payload
	// 转成通用 map，便于序列化成 JSON payload。
	taskMaps := make([]any, len(tasks))
	for i, t := range tasks {
		taskMaps[i] = t
	}
	streaming.Get().Publish(workflowID, streaming.Event{
		WorkflowID: workflowID,
		Type:       string(StreamEventTaskListUpdated),
		AgentID:    agentID,
		Message:    message,
		Payload:    map[string]any{"tasks": taskMaps},
		Timestamp:  time.Now(),
	})
}

// fetchAllTasks reads all tasks from the Redis hash for a workflow.
// fetchAllTasks 从 Redis hash 读取某个 workflow 的全部 task。
func fetchAllTasks(ctx context.Context, rc *redis.Client, workflowID string) []SwarmTask {
	key := taskListKey(workflowID)
	vals, err := rc.HGetAll(ctx, key).Result()
	if err != nil {
		return nil
	}
	tasks := make([]SwarmTask, 0, len(vals))
	for _, v := range vals {
		var t SwarmTask
		if err := json.Unmarshal([]byte(v), &t); err == nil {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

// ValidateStatusTransition checks whether a status transition is allowed.
// Valid transitions: pending->in_progress, in_progress->completed, pending->completed,
// completed->in_progress (reactivate for continuation), in_progress->in_progress (re-assign).
// ValidateStatusTransition 检查状态流转是否允许。
// 允许的流转：pending->in_progress、in_progress->completed、pending->completed、
// completed->in_progress（重新激活以继续）、in_progress->in_progress（重新指派）。
func ValidateStatusTransition(from, to string) error {
	switch {
	case from == "pending" && to == "in_progress":
		return nil
	case from == "in_progress" && to == "completed":
		return nil
	case from == "pending" && to == "completed":
		return nil // Lead can cancel pending tasks directly
		// Lead 可以直接取消 pending 的 task。
	case from == "in_progress" && to == "in_progress":
		return nil // Lead can re-assign an in_progress task
		// Lead 可以重新指派 in_progress 的 task。
	case from == "completed" && to == "in_progress":
		return nil // Reactivate: agent idle auto-completes task, then Lead assigns continuation
		// 重新激活：agent 空闲时会自动完成 task，之后 Lead 再指派后续工作。
	default:
		return fmt.Errorf("invalid status transition: %s -> %s", from, to)
	}
}

// InitTaskListInput is the input for bulk-initializing a task list.
// InitTaskListInput 是批量初始化 task list 的输入。
type InitTaskListInput struct {
	WorkflowID string      `json:"workflow_id"`
	Tasks      []SwarmTask `json:"tasks"`
}

// InitTaskList bulk-writes tasks into a Redis hash keyed by workflow ID.
// InitTaskList 按 workflow ID 把 task 批量写入 Redis hash。
func (a *Activities) InitTaskList(ctx context.Context, in InitTaskListInput) error {
	if in.WorkflowID == "" {
		return fmt.Errorf("invalid args: empty workflow_id")
	}
	if len(in.Tasks) == 0 {
		return fmt.Errorf("invalid args: empty tasks")
	}

	rc := a.sessionManager.RedisWrapper().GetClient()
	key := taskListKey(in.WorkflowID)

	fields := make(map[string]any, len(in.Tasks))
	for i := range in.Tasks {
		t := &in.Tasks[i]
		if t.ID == "" {
			return fmt.Errorf("task at index %d has empty ID", i)
		}
		if t.Status == "" {
			t.Status = "pending"
		}
		if t.CreatedAt == "" {
			t.CreatedAt = time.Now().UTC().Format(time.RFC3339)
		}
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("marshal task %s: %w", t.ID, err)
		}
		fields[t.ID] = b
	}

	if err := rc.HSet(ctx, key, fields).Err(); err != nil {
		return fmt.Errorf("HSET tasklist: %w", err)
	}
	rc.Expire(ctx, key, 24*time.Hour)

	// Emit streaming event with full task list (pass tasks directly, no re-read)
	// 发布带完整 task list 的 streaming event（直接传 tasks，不再回读）。
	publishTaskListUpdate(in.WorkflowID, "tasklist", fmt.Sprintf("Created %d tasks", len(in.Tasks)), in.Tasks)

	return nil
}

// GetTaskListInput is the input for retrieving all tasks.
// GetTaskListInput 是获取全部 task 的输入。
type GetTaskListInput struct {
	WorkflowID string `json:"workflow_id"`
}

// GetTaskList returns all tasks from the Redis hash, sorted by ID.
// GetTaskList 返回 Redis hash 中的全部 task，按 ID 排序。
func (a *Activities) GetTaskList(ctx context.Context, in GetTaskListInput) ([]SwarmTask, error) {
	if in.WorkflowID == "" {
		return nil, fmt.Errorf("invalid args: empty workflow_id")
	}

	rc := a.sessionManager.RedisWrapper().GetClient()
	key := taskListKey(in.WorkflowID)

	vals, err := rc.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("HGETALL tasklist: %w", err)
	}

	tasks := make([]SwarmTask, 0, len(vals))
	for _, v := range vals {
		var t SwarmTask
		if err := json.Unmarshal([]byte(v), &t); err != nil {
			continue // skip malformed entries
			// 跳过格式错误的 entry。
		}
		tasks = append(tasks, t)
	}

	sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })
	return tasks, nil
}

// UpdateTaskStatusInput is the input for changing a task's status.
// UpdateTaskStatusInput 是修改 task 状态的输入。
type UpdateTaskStatusInput struct {
	WorkflowID string `json:"workflow_id"`
	TaskID     string `json:"task_id"`
	Status     string `json:"status"` // target status
	// 目标状态。
	AgentID string `json:"agent_id"` // who is making the change
	// 发起变更的 agent。
}

// UpdateTaskStatus performs a validated status transition on a single task.
// UpdateTaskStatus 对单个 task 执行带校验的状态流转。
func (a *Activities) UpdateTaskStatus(ctx context.Context, in UpdateTaskStatusInput) error {
	if in.WorkflowID == "" || in.TaskID == "" || in.Status == "" {
		return fmt.Errorf("invalid args: workflow_id, task_id, and status are required")
	}

	rc := a.sessionManager.RedisWrapper().GetClient()
	key := taskListKey(in.WorkflowID)

	// Fetch current task
	// 读取当前 task。
	raw, err := rc.HGet(ctx, key, in.TaskID).Result()
	if err != nil {
		return fmt.Errorf("HGET task %s: %w", in.TaskID, err)
	}

	var task SwarmTask
	if err := json.Unmarshal([]byte(raw), &task); err != nil {
		return fmt.Errorf("unmarshal task %s: %w", in.TaskID, err)
	}

	// Validate transition
	// 校验状态流转。
	if err := ValidateStatusTransition(task.Status, in.Status); err != nil {
		return err
	}

	// Apply changes
	// 应用变更。
	task.Status = in.Status
	if in.Status == "in_progress" {
		task.Owner = in.AgentID
	}
	if in.Status == "completed" {
		task.CompletedAt = time.Now().UTC().Format(time.RFC3339)
		// Preserve owner: if task had no owner (e.g. pending→completed), record who completed it
		// 保留 owner：若 task 原本没有 owner（例如 pending→completed），记录是谁完成的。
		if task.Owner == "" && in.AgentID != "" {
			task.Owner = in.AgentID
		}
	}

	b, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task %s: %w", in.TaskID, err)
	}
	if err := rc.HSet(ctx, key, in.TaskID, b).Err(); err != nil {
		return fmt.Errorf("HSET task %s: %w", in.TaskID, err)
	}

	// Emit streaming event with full task list
	// 发布带完整 task list 的 streaming event。
	allTasks := fetchAllTasks(ctx, rc, in.WorkflowID)
	publishTaskListUpdate(in.WorkflowID, in.AgentID, "", allTasks)

	return nil
}

// UpdateTaskDescriptionInput is the input for modifying a task's description.
// UpdateTaskDescriptionInput 是修改 task 描述的输入。
type UpdateTaskDescriptionInput struct {
	WorkflowID  string `json:"workflow_id"`
	TaskID      string `json:"task_id"`
	Description string `json:"description"`
}

// UpdateTaskDescription modifies an existing task's description in Redis and emits TASKLIST_UPDATED.
// UpdateTaskDescription 修改 Redis 中已有 task 的描述，并发布 TASKLIST_UPDATED。
func (a *Activities) UpdateTaskDescription(ctx context.Context, in UpdateTaskDescriptionInput) error {
	if in.WorkflowID == "" || in.TaskID == "" || in.Description == "" {
		return fmt.Errorf("invalid args: workflow_id, task_id, and description are required")
	}

	rc := a.sessionManager.RedisWrapper().GetClient()
	key := taskListKey(in.WorkflowID)

	raw, err := rc.HGet(ctx, key, in.TaskID).Result()
	if err != nil {
		return fmt.Errorf("HGET task %s: %w", in.TaskID, err)
	}

	var task SwarmTask
	if err := json.Unmarshal([]byte(raw), &task); err != nil {
		return fmt.Errorf("unmarshal task %s: %w", in.TaskID, err)
	}

	task.Description = in.Description

	b, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task %s: %w", in.TaskID, err)
	}
	if err := rc.HSet(ctx, key, in.TaskID, b).Err(); err != nil {
		return fmt.Errorf("HSET task %s: %w", in.TaskID, err)
	}

	allTasks := fetchAllTasks(ctx, rc, in.WorkflowID)
	publishTaskListUpdate(in.WorkflowID, "lead", fmt.Sprintf("Updated task %s", in.TaskID), allTasks)

	return nil
}

// ClaimTaskInput is the input for atomically claiming a pending task.
// ClaimTaskInput 是原子认领 pending task 的输入。
type ClaimTaskInput struct {
	WorkflowID string `json:"workflow_id"`
	TaskID     string `json:"task_id"`
	AgentID    string `json:"agent_id"`
}

// ClaimTaskResult is the result of a claim attempt.
// ClaimTaskResult 是一次认领尝试的结果。
type ClaimTaskResult struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// claimTaskScript is a Redis Lua script that atomically claims a pending task (D10).
// claimTaskScript 是原子认领 pending task 的 Redis Lua 脚本（D10）。
var claimTaskScript = `
local key = KEYS[1]
local task_id = ARGV[1]
local agent_id = ARGV[2]
local current = redis.call('HGET', key, task_id)
if current then
    local task = cjson.decode(current)
    if task.status == 'pending' and (task.owner == nil or task.owner == '' or task.owner == cjson.null) then
        task.status = 'in_progress'
        task.owner = agent_id
        redis.call('HSET', key, task_id, cjson.encode(task))
        return 1
    end
end
return 0
`

// ClaimTask atomically claims a pending task for an agent using a Redis Lua script (D10).
// ClaimTask 用 Redis Lua 脚本为 agent 原子认领 pending task（D10）。
func (a *Activities) ClaimTask(ctx context.Context, in ClaimTaskInput) (ClaimTaskResult, error) {
	if in.WorkflowID == "" || in.TaskID == "" || in.AgentID == "" {
		return ClaimTaskResult{}, fmt.Errorf("invalid args: workflow_id, task_id, and agent_id are required")
	}

	rc := a.sessionManager.RedisWrapper().GetClient()
	key := fmt.Sprintf("wf:%s:tasklist", in.WorkflowID)

	result, err := rc.Eval(ctx, claimTaskScript, []string{key}, in.TaskID, in.AgentID).Int()
	if err != nil {
		return ClaimTaskResult{}, fmt.Errorf("claim task Lua script failed: %w", err)
	}

	if result == 1 {
		// Emit streaming event with full task list
		// 发布带完整 task list 的 streaming event。
		allTasks := fetchAllTasks(ctx, rc, in.WorkflowID)
		publishTaskListUpdate(in.WorkflowID, in.AgentID, "", allTasks)
		return ClaimTaskResult{Success: true, Message: fmt.Sprintf("Task %s claimed by %s", in.TaskID, in.AgentID)}, nil
	}
	return ClaimTaskResult{Success: false, Message: fmt.Sprintf("Task %s not available for claiming", in.TaskID)}, nil
}

// CreateTaskInput is the input for adding a new task to the list.
// CreateTaskInput 是向 task list 添加新 task 的输入。
type CreateTaskInput struct {
	WorkflowID string    `json:"workflow_id"`
	Task       SwarmTask `json:"task"`
}

// CreateTask adds a new task to the Redis hash.
// CreateTask 把新 task 加入 Redis hash。
func (a *Activities) CreateTask(ctx context.Context, in CreateTaskInput) error {
	if in.WorkflowID == "" || in.Task.ID == "" {
		return fmt.Errorf("invalid args: workflow_id and task.id are required")
	}

	rc := a.sessionManager.RedisWrapper().GetClient()
	key := taskListKey(in.WorkflowID)

	// Check for duplicate
	// 检查是否重复。
	exists, err := rc.HExists(ctx, key, in.Task.ID).Result()
	if err != nil {
		return fmt.Errorf("HEXISTS task %s: %w", in.Task.ID, err)
	}
	if exists {
		return fmt.Errorf("task %s already exists", in.Task.ID)
	}

	// Set defaults
	// 设置默认值。
	if in.Task.Status == "" {
		in.Task.Status = "pending"
	}
	if in.Task.CreatedAt == "" {
		in.Task.CreatedAt = time.Now().UTC().Format(time.RFC3339)
	}

	b, err := json.Marshal(in.Task)
	if err != nil {
		return fmt.Errorf("marshal task %s: %w", in.Task.ID, err)
	}
	if err := rc.HSet(ctx, key, in.Task.ID, b).Err(); err != nil {
		return fmt.Errorf("HSET task %s: %w", in.Task.ID, err)
	}

	// Emit streaming event with full task list
	// 发布带完整 task list 的 streaming event。
	allTasks := fetchAllTasks(ctx, rc, in.WorkflowID)
	publishTaskListUpdate(in.WorkflowID, "tasklist", "", allTasks)

	return nil
}
