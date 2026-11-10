package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrClaimNotFound = errors.New("claim not found")

const claimKeyPrefix = "shannon:daemon:claim:"

// ClaimMetadata stored alongside each claim in Redis.
// ClaimMetadata 与每个 claim 一起存储在 Redis 中。
type ClaimMetadata struct {
	ConnID        string `json:"conn_id"`
	ChannelID     string `json:"channel_id,omitempty"`
	ChannelType   string `json:"channel_type"`
	ThreadID      string `json:"thread_id,omitempty"`
	ReplyToken    string `json:"reply_token,omitempty"`
	AgentName     string `json:"agent_name,omitempty"`
	Timestamp     string `json:"timestamp"`
	WorkflowID    string `json:"workflow_id,omitempty"`
	WorkflowRunID string `json:"workflow_run_id,omitempty"`
}

// ClaimManager coordinates message claims via Redis.
type ClaimManager struct {
	rc       redis.Cmdable
	claimTTL time.Duration // initial claim TTL (default 60s)
}

func NewClaimManager(rc redis.Cmdable) *ClaimManager {
	return &ClaimManager{rc: rc, claimTTL: 60 * time.Second}
}

// SetClaimTTL configures the initial claim TTL and heartbeat extension duration.
// SetClaimTTL 配置 claim 的初始 TTL 与心跳续期时长。
func (cm *ClaimManager) SetClaimTTL(ttl time.Duration) {
	if ttl > 0 {
		cm.claimTTL = ttl
	}
}

func claimKey(messageID string) string {
	return fmt.Sprintf("%s%s", claimKeyPrefix, messageID)
}

// TryClaim atomically claims a message. Returns true if this caller won.
// TryClaim 原子地占用一条消息。若本次调用方胜出则返回 true。
func (cm *ClaimManager) TryClaim(ctx context.Context, messageID string, meta ClaimMetadata) (bool, error) {
	data, err := json.Marshal(meta)
	if err != nil {
		return false, err
	}
	ok, err := cm.rc.SetNX(ctx, claimKey(messageID), data, cm.claimTTL).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

// ExtendClaim extends the TTL on an existing claim (progress heartbeat).
// ExtendClaim 延长已有 claim 的 TTL（进度心跳）。
func (cm *ClaimManager) ExtendClaim(ctx context.Context, messageID string, ttl time.Duration) error {
	return cm.rc.Expire(ctx, claimKey(messageID), ttl).Err()
}

// GetClaim retrieves claim metadata.
func (cm *ClaimManager) GetClaim(ctx context.Context, messageID string) (ClaimMetadata, error) {
	data, err := cm.rc.Get(ctx, claimKey(messageID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ClaimMetadata{}, ErrClaimNotFound
		}
		return ClaimMetadata{}, err
	}
	var meta ClaimMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return ClaimMetadata{}, err
	}
	return meta, nil
}

// UpdateClaim updates the claim metadata in Redis without changing TTL.
// UpdateClaim 更新 Redis 中的 claim metadata，不改变 TTL。
func (cm *ClaimManager) UpdateClaim(ctx context.Context, messageID string, meta ClaimMetadata) error {
	key := claimKey(messageID)
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal claim: %w", err)
	}
	ttl, err := cm.rc.TTL(ctx, key).Result()
	if err != nil || ttl <= 0 {
		return ErrClaimNotFound
	}
	return cm.rc.Set(ctx, key, data, ttl).Err()
}

// ReleaseClaim deletes a claim (after reply or disconnect).
// ReleaseClaim 删除 claim（回复后或断连后）。
func (cm *ClaimManager) ReleaseClaim(ctx context.Context, messageID string) error {
	return cm.rc.Del(ctx, claimKey(messageID)).Err()
}

// ReleaseAllForConn releases all claims held by a specific connection.
// ReleaseAllForConn 释放某个连接持有的全部 claim。
func (cm *ClaimManager) ReleaseAllForConn(ctx context.Context, connID string) ([]string, error) {
	var released []string
	iter := cm.rc.Scan(ctx, 0, claimKeyPrefix+"*", 100).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		data, err := cm.rc.Get(ctx, key).Bytes()
		if err != nil {
			continue
		}
		var meta ClaimMetadata
		if err := json.Unmarshal(data, &meta); err != nil {
			continue
		}
		if meta.ConnID == connID {
			cm.rc.Del(ctx, key)
			msgID := key[len(claimKeyPrefix):]
			released = append(released, msgID)
		}
	}
	return released, iter.Err()
}
