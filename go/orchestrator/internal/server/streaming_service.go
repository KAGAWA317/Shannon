package server

import (
	"context"
	"time"

	pb "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/orchestrator"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/streaming"
	serviceerror "go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// StreamingServiceServer implements the gRPC StreamingService backed by the in-process manager.
// StreamingServiceServer 实现由进程内 manager 支撑的 gRPC StreamingService。
type StreamingServiceServer struct {
	pb.UnimplementedStreamingServiceServer
	mgr     *streaming.Manager
	logger  *zap.Logger
	tclient client.Client
}

func NewStreamingService(mgr *streaming.Manager, logger *zap.Logger) *StreamingServiceServer {
	return &StreamingServiceServer{mgr: mgr, logger: logger}
}

// SetTemporalClient allows wiring the Temporal client after service construction.
// SetTemporalClient 允许在服务构造后注入 Temporal client。
func (s *StreamingServiceServer) SetTemporalClient(c client.Client) {
	s.tclient = c
}

func (s *StreamingServiceServer) StreamTaskExecution(req *pb.StreamRequest, srv pb.StreamingService_StreamTaskExecutionServer) error {
	wf := req.GetWorkflowId()
	if wf == "" {
		return nil
	}
	// Build type filter set
	typeFilter := map[string]struct{}{}
	for _, t := range req.GetTypes() {
		if t != "" {
			typeFilter[t] = struct{}{}
		}
	}

	var lastSentStreamID string
	firstEventSeen := false

	// Replay based on stream ID or seq (prefer stream ID)
	// 按 stream ID 或 seq 回放（优先 stream ID）
	if req.GetLastStreamId() != "" {
		// Resume from Redis stream ID
		for _, ev := range s.mgr.ReplayFromStreamID(wf, req.GetLastStreamId()) {
			// Mark that at least one event exists (even if filtered out)
			// 标记至少存在一个 event（即使被过滤掉）
			firstEventSeen = true
			if len(typeFilter) > 0 {
				if _, ok := typeFilter[ev.Type]; !ok {
					continue
				}
			}
			if ev.StreamID != "" {
				lastSentStreamID = ev.StreamID
			}
			if err := srv.Send(toProto(ev)); err != nil {
				return err
			}
		}
	} else if req.GetLastEventId() > 0 {
		// Backward compat: resume from numeric seq
		for _, ev := range s.mgr.ReplaySince(wf, req.GetLastEventId()) {
			// Mark that at least one event exists (even if filtered out)
			// 标记至少存在一个 event（即使被过滤掉）
			firstEventSeen = true
			if len(typeFilter) > 0 {
				if _, ok := typeFilter[ev.Type]; !ok {
					continue
				}
			}
			if ev.StreamID != "" {
				lastSentStreamID = ev.StreamID
			}
			if err := srv.Send(toProto(ev)); err != nil {
				return err
			}
		}
	}

	// Subscribe to live events, avoiding gaps
	startFrom := "$" // Default to new messages only
	if lastSentStreamID != "" {
		// Continue from last replayed message
		startFrom = lastSentStreamID
	} else if req.GetLastStreamId() == "" && req.GetLastEventId() == 0 {
		// No resume point, start from beginning
		startFrom = "0-0"
	}
	ch := s.mgr.SubscribeFrom(wf, 256, startFrom)
	defer s.mgr.Unsubscribe(wf, ch)

	// First-event timeout for invalid/non-existent workflows
	// 针对无效/不存在 workflow 的首 event timeout
	firstEventTimer := time.NewTimer(30 * time.Second)
	defer firstEventTimer.Stop()

	// Stream live events
	for {
		select {
		case <-srv.Context().Done():
			return nil
		case <-firstEventTimer.C:
			if !firstEventSeen {
				if s.tclient == nil {
					s.logger.Warn("First-event timeout but Temporal client not available", zap.String("workflow_id", wf))
					return grpcstatus.Error(codes.Internal, "workflow validation unavailable")
				}
				ctx, cancel := context.WithTimeout(srv.Context(), 2*time.Second)
				_, err := s.tclient.DescribeWorkflowExecution(ctx, wf, "")
				cancel()
				if err != nil {
					if _, ok := err.(*serviceerror.NotFound); ok {
						return grpcstatus.Error(codes.NotFound, "workflow not found")
					}
					// Other errors (timeout, etc) also indicate invalid workflow
					// 其它错误（timeout 等）同样说明 workflow 不存在
					return grpcstatus.Error(codes.NotFound, "workflow not found or unavailable")
				}
				// Workflow exists but no events yet - reset timer and keep waiting
				// workflow 存在但尚无 event——重置计时器并继续等待
				firstEventTimer.Reset(30 * time.Second)
			}
		case ev, ok := <-ch:
			if !ok {
				// Channel closed unexpectedly
				return nil
			}

			// Check for WORKFLOW_COMPLETED before filtering to ensure stream closes
			// 在过滤之前检查 WORKFLOW_COMPLETED，确保流能关闭
			isCompleted := ev.Type == "WORKFLOW_COMPLETED"

			// Any incoming event means the workflow exists; disable first-event detection
			// 任何到达的 event 都说明 workflow 存在；关闭首 event 检测
			if !firstEventSeen {
				firstEventSeen = true
			}

			// Apply type filter
			if len(typeFilter) > 0 {
				if _, ok := typeFilter[ev.Type]; !ok {
					// Skip sending this event, but still close on completion
					// 跳过发送该 event，但仍在完成时关闭
					if isCompleted {
						return nil
					}
					continue
				}
			}

			// Send event
			if err := srv.Send(toProto(ev)); err != nil {
				return err
			}

			// Close stream after WORKFLOW_COMPLETED
			if isCompleted {
				return nil
			}
		}
	}
}

func toProto(ev streaming.Event) *pb.TaskUpdate {
	ts := ev.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	return &pb.TaskUpdate{
		WorkflowId: ev.WorkflowID,
		Type:       ev.Type,
		AgentId:    ev.AgentID,
		Message:    ev.Message,
		Timestamp:  timestamppb.New(ts),
		Seq:        ev.Seq,
		StreamId:   ev.StreamID,
	}
}
