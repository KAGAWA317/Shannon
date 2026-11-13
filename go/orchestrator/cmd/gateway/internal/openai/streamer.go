package openai

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// ShannonSSEEvent represents an event from Shannon's internal SSE stream.
// This is used for parsing incoming events, distinct from the output ShannonEvent in types.go.
// ShannonSSEEvent 表示 Shannon 内部 SSE stream 中的 event，用于解析输入 event，
// 与 types.go 中输出的 ShannonEvent 区分。
type ShannonSSEEvent struct {
	WorkflowID string         `json:"workflow_id"`
	Type       string         `json:"type"`
	AgentID    string         `json:"agent_id,omitempty"`
	Message    string         `json:"message,omitempty"`
	Seq        int64          `json:"seq"`
	StreamID   string         `json:"stream_id,omitempty"`
	Delta      string         `json:"delta,omitempty"`    // For LLM_PARTIAL events
	Response   string         `json:"response,omitempty"` // For LLM_OUTPUT events
	Payload    map[string]any `json:"payload,omitempty"`  // Additional event data
	Metadata   *struct {
		TokensUsed   int     `json:"tokens_used,omitempty"`
		InputTokens  int     `json:"input_tokens,omitempty"`
		OutputTokens int     `json:"output_tokens,omitempty"`
		CostUSD      float64 `json:"cost_usd,omitempty"`
		ModelUsed    string  `json:"model_used,omitempty"`
		Provider     string  `json:"provider,omitempty"`
	} `json:"metadata,omitempty"`
}

// Streamer transforms Shannon SSE events to OpenAI format.
type Streamer struct {
	logger          *zap.Logger
	completionID    string
	modelName       string
	created         int64
	sentRole        bool
	seenPartial     bool
	totalTokens     *Usage
	metrics         *MetricsRecorder
	firstTokenSent  bool
	sawFinalOutput  bool
	lastClientWrite time.Time // Track last write to client for keepalive timing
}

// NewStreamer creates a new response streamer.
func NewStreamer(logger *zap.Logger, modelName string) *Streamer {
	return &Streamer{
		logger:          logger,
		completionID:    GenerateCompletionID(),
		modelName:       modelName,
		created:         time.Now().Unix(),
		sentRole:        false,
		totalTokens:     &Usage{},
		lastClientWrite: time.Now(),
	}
}

// NewStreamerWithMetrics creates a new response streamer with metrics recording.
func NewStreamerWithMetrics(logger *zap.Logger, modelName string, metrics *MetricsRecorder) *Streamer {
	return &Streamer{
		logger:          logger,
		completionID:    GenerateCompletionID(),
		modelName:       modelName,
		created:         time.Now().Unix(),
		sentRole:        false,
		totalTokens:     &Usage{},
		metrics:         metrics,
		lastClientWrite: time.Now(),
	}
}

// HeartbeatInterval is the interval at which to send SSE keepalive comments.
// Set to 30s to stay well under typical ALB idle timeouts (60-300s).
// HeartbeatInterval 是发送 SSE keepalive 注释的间隔；设为 30 秒，远低于常见的 ALB 空闲 timeout（60-300 秒）。
const HeartbeatInterval = 30 * time.Second

// sseLineResult represents a line read from the SSE scanner.
type sseLineResult struct {
	line string
	err  error
}

// StreamResponse reads from Shannon SSE and writes OpenAI-format chunks.
func (s *Streamer) StreamResponse(ctx context.Context, sseReader io.Reader, w http.ResponseWriter, includeUsage bool) error {
	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // Disable nginx buffering

	flusher, ok := w.(http.Flusher)
	if !ok {
		return fmt.Errorf("streaming not supported")
	}

	// Use a goroutine to read lines so we can implement heartbeat timeouts.
	// Channel is buffered (1) to allow goroutine to exit even if receiver stops.
	// 用 goroutine 读行，才能实现心跳 timeout；channel 缓冲为 1，接收方停止后 goroutine 也能退出。
	lineCh := make(chan sseLineResult, 1)
	go func() {
		defer close(lineCh)
		scanner := bufio.NewScanner(sseReader)
		// Allow large SSE events (e.g., browser action=screenshot base64 payloads).
		// Configurable via env to accommodate different deployments.
		// 允许较大的 SSE event（例如 browser action=screenshot 的 base64 payload）；可通过环境变量按部署调整。
		bufBytes := 64 * 1024
		if v := os.Getenv("OPENAI_SSE_SCANNER_BUF_BYTES"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				bufBytes = n
			}
		}
		maxBytes := 16 * 1024 * 1024
		if v := os.Getenv("OPENAI_SSE_SCANNER_MAX_BYTES"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				maxBytes = n
			}
		}
		scanner.Buffer(make([]byte, 0, bufBytes), maxBytes)
		for scanner.Scan() {
			select {
			case lineCh <- sseLineResult{line: scanner.Text()}:
			case <-ctx.Done():
				// Context canceled, stop reading
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case lineCh <- sseLineResult{err: err}:
			case <-ctx.Done():
			}
		}
	}()

	var eventType string
	var eventData string
	stoppedEarly := false
	heartbeatTicker := time.NewTicker(HeartbeatInterval)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Send final done message
			s.writeDone(w, flusher)
			return ctx.Err()

		case <-heartbeatTicker.C:
			// Only send keepalive if we haven't written to client recently.
			// This prevents keepalive gaps when upstream sends frequent pings
			// but we're not forwarding content to the client.
			// 只有最近没有向客户端写过数据时才发 keepalive，避免上游频繁 ping 而我们并未转发内容时出现心跳空档。
			if time.Since(s.lastClientWrite) >= HeartbeatInterval {
				s.writeHeartbeat(w, flusher)
			}

		case result, ok := <-lineCh:
			if !ok {
				// Channel closed, scanner finished
				goto done
			}

			if result.err != nil {
				s.logger.Error("Scanner error", zap.Error(result.err))
				s.writeFinalChunk(w, flusher, includeUsage)
				s.writeDone(w, flusher)
				return result.err
			}

			line := result.line

			// Forward upstream SSE comments (like ": ping") as keepalives to client.
			// This ensures client connection stays alive during long quiet periods.
			// 把上游的 SSE 注释（如 ": ping"）作为 keepalive 转发给客户端，保证长时间静默期间连接不被断开。
			if strings.HasPrefix(line, ":") {
				s.writeHeartbeat(w, flusher)
				continue
			}

			// Parse SSE format
			if after, ok0 := strings.CutPrefix(line, "event:"); ok0 {
				eventType = strings.TrimSpace(after)
			} else if after, ok0 := strings.CutPrefix(line, "data:"); ok0 {
				eventData = strings.TrimSpace(after)
			} else if line == "" && eventData != "" {
				// End of event, process it
				if err := s.processEvent(eventType, eventData, w, flusher); err != nil {
					if errors.Is(err, errStreamEnd) {
						stoppedEarly = true
						goto done
					}
					s.logger.Debug("Error processing event", zap.Error(err))
				}
				eventType = ""
				eventData = ""
			}
		}
	}

done:
	if stoppedEarly {
		s.logger.Debug("Upstream stream ended early")
	}

	// Send final chunk with finish_reason and done
	s.writeFinalChunk(w, flusher, includeUsage)
	s.writeDone(w, flusher)

	return nil
}

var errStreamEnd = errors.New("stream ended")

// processEvent handles a single Shannon event.
func (s *Streamer) processEvent(eventType, data string, w http.ResponseWriter, flusher http.Flusher) error {
	var event ShannonSSEEvent
	if err := json.Unmarshal([]byte(data), &event); err != nil {
		// Try to use raw data as message
		event.Message = data
	}

	// Determine the effective event type (prefer event.Type from JSON, fall back to SSE event name)
	// 确定实际的 event 类型（优先用 JSON 里的 event.Type，回退到 SSE event 名）。
	effectiveType := event.Type
	if effectiveType == "" {
		effectiveType = eventType
	}

	switch {
	case isErrorEvent(eventType, event.Type):
		msg := strings.TrimSpace(event.Message)
		if msg == "" {
			msg = "Workflow error"
		}
		s.writeErrorChunk(w, flusher, msg)
		return errStreamEnd

	case isPartialEvent(eventType, event.Type):
		content := event.Delta
		if content == "" {
			content = event.Message
		}
		// Only stream partials from final-phase agents (synthesis).
		// Skip subtask agent partials (they set seenPartial and hide final output).
		// Skip citation_agent partials (they contain <cited_report> tags).
		// 只 stream 转发最终阶段 agent（synthesis）的 partial；跳过子 task agent 的 partial（它们会设置 seenPartial 并隐藏最终输出），
		// 也跳过 citation_agent 的 partial（其中含 <cited_report> 标签）。
		if !isFinalPhaseAgent(event.AgentID) {
			s.logger.Debug("Skipping non-final agent partial",
				zap.String("agent_id", event.AgentID))
			return nil
		}
		if strings.TrimSpace(content) != "" {
			s.seenPartial = true
			s.writeContentChunk(w, flusher, content)
		}
		return nil

	case isOutputEvent(eventType, event.Type):
		// Only show "final_output" LLM_OUTPUT - the canonical answer after all processing.
		// Skip all other LLM_OUTPUT events to avoid duplicates from intermediate phases.
		// 只展示 "final_output" 的 LLM_OUTPUT——即所有处理完成后的规范答案；
		// 其余 LLM_OUTPUT 一律跳过，避免中间阶段造成重复。
		if event.AgentID == "final_output" {
			content := event.Response
			if content == "" {
				content = event.Message
			}
			if strings.TrimSpace(content) != "" {
				// B-lite: Stream final output in chunks for progressive display.
				// This gives streaming UX while ensuring correctness (single canonical answer).
				// B-lite：把最终输出分块 streaming 发送，兼顾渐进展示与正确性（只有一份规范答案）。
				s.writeContentChunked(w, flusher, content)
				s.sawFinalOutput = true
			}
		} else {
			s.logger.Debug("Skipping non-final LLM_OUTPUT",
				zap.String("agent_id", event.AgentID))
		}
		s.captureUsage(event.Metadata)
		return nil

	case isEndEvent(eventType, event.Type):
		// Some workflows may emit WORKFLOW_COMPLETED before emitting the canonical "final_output" LLM_OUTPUT.
		// If we haven't seen final output yet, keep the stream open until STREAM_END or EOF.
		// 某些 workflow 会先发 WORKFLOW_COMPLETED 再发规范的 "final_output" LLM_OUTPUT；
		// 若尚未见到最终输出，则保持 stream 打开，直到 STREAM_END 或 EOF。
		if isWorkflowCompletedEvent(eventType, event.Type) && !s.sawFinalOutput {
			s.writeEventChunk(w, flusher, "WORKFLOW_COMPLETED", event)
			return nil
		}
		return errStreamEnd

	case isAgentEvent(effectiveType):
		// Forward agent lifecycle and progress events
		s.writeEventChunk(w, flusher, effectiveType, event)
		return nil

	default:
		s.logger.Debug("Ignoring event type", zap.String("type", effectiveType))
		return nil
	}
}

func isErrorEvent(eventName, eventType string) bool {
	return eventName == "error" || eventName == "ERROR_OCCURRED" || eventType == "ERROR_OCCURRED" || eventType == "WORKFLOW_FAILED"
}

func isPartialEvent(eventName, eventType string) bool {
	return eventName == "thread.message.delta" || eventName == "LLM_PARTIAL" || eventType == "LLM_PARTIAL"
}

func isOutputEvent(eventName, eventType string) bool {
	return eventName == "thread.message.completed" || eventName == "LLM_OUTPUT" || eventType == "LLM_OUTPUT"
}

func isEndEvent(eventName, eventType string) bool {
	return eventName == "done" || eventName == "STREAM_END" || eventName == "WORKFLOW_COMPLETED" || eventType == "STREAM_END" || eventType == "WORKFLOW_COMPLETED"
}

func isWorkflowCompletedEvent(eventName, eventType string) bool {
	return eventName == "WORKFLOW_COMPLETED" || eventType == "WORKFLOW_COMPLETED"
}

// isFinalPhaseAgent returns true if the agent_id represents a final-phase agent
// whose partials should be streamed to the OpenAI client.
// For maximum correctness, we drop ALL partials and rely on the "final_output"
// LLM_OUTPUT event which contains the fully post-processed answer.
// isFinalPhaseAgent 判断 agent_id 是否属于最终阶段（其 partial 应转发给 OpenAI 客户端）。
// 为最大程度保证正确性，这里丢弃所有 partial，只依赖 "final_output" 的 LLM_OUTPUT event——它包含后处理完成的答案。
func isFinalPhaseAgent(agentID string) bool {
	// Drop all partials - they can cause duplicates and include raw intermediate work.
	// The canonical final answer comes via "final_output" LLM_OUTPUT event.
	// Subtask agents (Tenma, Gora, etc.) are intermediate work.
	// synthesis partials may duplicate final_output.
	// citation_agent partials contain <cited_report> wrapper tags.
	// 丢弃所有 partial：它们会带来重复，且包含原始中间产物。规范答案只来自 "final_output" 的 LLM_OUTPUT event。
	// 子 task agent（Tenma、Gora 等）属于中间过程；synthesis 的 partial 可能与 final_output 重复；citation_agent 的 partial 带 <cited_report> 包裹标签。
	return false
}

// isAgentEvent returns true for agent lifecycle and progress events that should be forwarded.
func isAgentEvent(eventType string) bool {
	switch eventType {
	case "WORKFLOW_STARTED",
		"AGENT_STARTED",
		"AGENT_COMPLETED",
		"AGENT_THINKING",
		"TOOL_INVOKED",
		"TOOL_OBSERVATION",
		"PROGRESS",
		"DATA_PROCESSING",
		"WAITING",
		"ERROR_RECOVERY",
		"TEAM_RECRUITED",
		"TEAM_RETIRED",
		"ROLE_ASSIGNED",
		"DELEGATION",
		"DEPENDENCY_SATISFIED",
		"BUDGET_THRESHOLD",
		"TEAM_STATUS",
		"APPROVAL_REQUESTED",
		"APPROVAL_DECISION",
		"WORKFLOW_PAUSING",
		"WORKFLOW_PAUSED",
		"WORKFLOW_RESUMED",
		"WORKFLOW_CANCELLING",
		"WORKFLOW_CANCELLED":
		return true
	default:
		return false
	}
}

func (s *Streamer) captureUsage(meta *struct {
	TokensUsed   int     `json:"tokens_used,omitempty"`
	InputTokens  int     `json:"input_tokens,omitempty"`
	OutputTokens int     `json:"output_tokens,omitempty"`
	CostUSD      float64 `json:"cost_usd,omitempty"`
	ModelUsed    string  `json:"model_used,omitempty"`
	Provider     string  `json:"provider,omitempty"`
}) {
	if meta == nil {
		return
	}
	s.totalTokens.PromptTokens = meta.InputTokens
	s.totalTokens.CompletionTokens = meta.OutputTokens
	s.totalTokens.TotalTokens = meta.TokensUsed
	if s.totalTokens.TotalTokens == 0 {
		s.totalTokens.TotalTokens = s.totalTokens.PromptTokens + s.totalTokens.CompletionTokens
	}
}

// FinalOutputChunkSize is the size of chunks for streaming final output.
// Smaller chunks give smoother streaming UX; larger chunks reduce overhead.
// FinalOutputChunkSize 是最终输出 streaming 分块的大小；块越小越顺滑，越大开销越低。
const FinalOutputChunkSize = 100 // ~100 chars per chunk for natural reading pace

// writeContentChunked splits content into chunks for progressive streaming.
// This gives OpenAI-compatible streaming UX while ensuring correctness.
// writeContentChunked 把内容切块逐步发送，在保证正确性的同时提供 OpenAI 兼容的 streaming 体验。
func (s *Streamer) writeContentChunked(w http.ResponseWriter, flusher http.Flusher, content string) {
	// Split content into chunks for progressive display
	runes := []rune(content)
	for i := 0; i < len(runes); i += FinalOutputChunkSize {
		end := min(i+FinalOutputChunkSize, len(runes))
		chunk := string(runes[i:end])
		s.writeContentChunk(w, flusher, chunk)
	}
}

// writeContentChunk writes a content delta chunk.
func (s *Streamer) writeContentChunk(w http.ResponseWriter, flusher http.Flusher, content string) {
	chunk := ChatCompletionChunk{
		ID:      s.completionID,
		Object:  "chat.completion.chunk",
		Created: s.created,
		Model:   s.modelName,
		Choices: []Choice{
			{
				Index: 0,
				Delta: &ChatDelta{},
			},
		},
	}

	// First chunk includes role
	if !s.sentRole {
		chunk.Choices[0].Delta.Role = "assistant"
		s.sentRole = true
	}

	// Record time to first token
	if !s.firstTokenSent && s.metrics != nil {
		s.metrics.RecordFirstToken()
		s.firstTokenSent = true
	}

	chunk.Choices[0].Delta.Content = content

	s.writeChunk(w, flusher, chunk)

	// Record chunk metric
	if s.metrics != nil {
		s.metrics.RecordStreamChunk()
	}
}

// writeFinalChunk writes the final chunk with finish_reason.
func (s *Streamer) writeFinalChunk(w http.ResponseWriter, flusher http.Flusher, includeUsage bool) {
	chunk := ChatCompletionChunk{
		ID:      s.completionID,
		Object:  "chat.completion.chunk",
		Created: s.created,
		Model:   s.modelName,
		Choices: []Choice{
			{
				Index:        0,
				Delta:        &ChatDelta{},
				FinishReason: "stop",
			},
		},
	}

	// Include usage in final chunk if requested
	if includeUsage && (s.totalTokens.TotalTokens > 0 || s.totalTokens.PromptTokens > 0) {
		chunk.Usage = s.totalTokens
	}

	s.writeChunk(w, flusher, chunk)
}

// writeErrorChunk writes an error as a final chunk.
func (s *Streamer) writeErrorChunk(w http.ResponseWriter, flusher http.Flusher, message string) {
	chunk := ChatCompletionChunk{
		ID:      s.completionID,
		Object:  "chat.completion.chunk",
		Created: s.created,
		Model:   s.modelName,
		Choices: []Choice{
			{
				Index:        0,
				Delta:        &ChatDelta{Content: "\n\n[Error: " + message + "]"},
				FinishReason: "stop",
			},
		},
	}

	s.writeChunk(w, flusher, chunk)
}

// writeChunk writes a single chunk to the response.
func (s *Streamer) writeChunk(w http.ResponseWriter, flusher http.Flusher, chunk ChatCompletionChunk) {
	data, err := json.Marshal(chunk)
	if err != nil {
		s.logger.Error("Failed to marshal chunk", zap.Error(err))
		return
	}

	fmt.Fprintf(w, "data: %s\n\n", data)
	flusher.Flush()
	s.lastClientWrite = time.Now()
}

// writeEventChunk writes an agent event chunk (no content, just shannon_events).
func (s *Streamer) writeEventChunk(w http.ResponseWriter, flusher http.Flusher, eventType string, event ShannonSSEEvent) {
	chunk := ChatCompletionChunk{
		ID:      s.completionID,
		Object:  "chat.completion.chunk",
		Created: s.created,
		Model:   s.modelName,
		Choices: []Choice{
			{
				Index: 0,
				Delta: &ChatDelta{}, // Empty delta for event-only chunks
			},
		},
		ShannonEvents: []ShannonEvent{
			{
				Type:      eventType,
				AgentID:   event.AgentID,
				Message:   event.Message,
				Timestamp: time.Now().Unix(),
				Payload:   event.Payload,
			},
		},
	}

	// First chunk includes role (if not already sent)
	if !s.sentRole {
		chunk.Choices[0].Delta.Role = "assistant"
		s.sentRole = true
	}

	s.writeChunk(w, flusher, chunk)
}

// writeHeartbeat writes an SSE comment as a keepalive signal.
// SSE comments start with ':' and are ignored by conforming clients.
// writeHeartbeat 写入 SSE 注释作为 keepalive；SSE 注释以 ':' 开头，合规客户端会忽略。
func (s *Streamer) writeHeartbeat(w http.ResponseWriter, flusher http.Flusher) {
	fmt.Fprintf(w, ": keepalive\n\n")
	flusher.Flush()
	s.lastClientWrite = time.Now()
	s.logger.Debug("Sent SSE heartbeat")
}

// writeDone writes the final [DONE] message.
func (s *Streamer) writeDone(w http.ResponseWriter, flusher http.Flusher) {
	fmt.Fprintf(w, "data: [DONE]\n\n")
	flusher.Flush()
}

// GetUsage returns the accumulated usage statistics.
func (s *Streamer) GetUsage() *Usage {
	return s.totalTokens
}

// GetCompletionID returns the completion ID used for this stream.
func (s *Streamer) GetCompletionID() string {
	return s.completionID
}
