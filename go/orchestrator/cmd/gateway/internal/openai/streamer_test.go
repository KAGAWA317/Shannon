package openai

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestStreamer_DoesNotEndOnWorkflowCompletedBeforeFinalOutput(t *testing.T) {
	sse := strings.Join([]string{
		"event: WORKFLOW_COMPLETED",
		`data: {"workflow_id":"wf-1","type":"WORKFLOW_COMPLETED","agent_id":"research","message":"done","seq":1}`,
		"",
		"event: thread.message.completed",
		`data: {"workflow_id":"wf-1","agent_id":"final_output","response":"Hello","seq":2}`,
		"",
		"event: done",
		"data: [DONE]",
		"",
	}, "\n")

	w := httptest.NewRecorder()
	streamer := NewStreamer(zap.NewNop(), "test-model")
	err := streamer.StreamResponse(context.Background(), strings.NewReader(sse), w, false)
	if err != nil {
		t.Fatalf("StreamResponse error: %v", err)
	}

	if !strings.Contains(w.Body.String(), `"content":"Hello"`) {
		t.Fatalf("expected streamed content in output, got:\n%s", w.Body.String())
	}
}
