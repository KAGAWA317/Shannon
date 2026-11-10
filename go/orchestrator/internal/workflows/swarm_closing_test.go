package workflows

import (
	"strings"
	"testing"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/stretchr/testify/assert"
)

func TestBuildClosingSummary(t *testing.T) {
	results := map[string]AgentLoopResult{
		"Akiba": {
			AgentID:  "Akiba",
			Role:     "coder",
			Response: "Implemented the API server with 3 endpoints for user management.",
			Success:  true,
		},
		"Maji": {
			AgentID:  "Maji",
			Role:     "coder",
			Response: "Created unit tests for all endpoints. Tests pass.",
			Success:  true,
		},
	}

	files := []activities.WorkspaceMaterial{
		{Path: "api/server.py", Content: "content here", Truncated: false},
		{Path: "tests/test_api.py", Content: "test content", Truncated: false},
	}

	currentRunFiles := map[string]bool{
		"api/server.py":     true,
		"tests/test_api.py": true,
	}
	summary := buildClosingSummary(results, files, currentRunFiles)

	assert.Contains(t, summary, "2 agents completed")
	assert.Contains(t, summary, "Akiba")
	assert.Contains(t, summary, "Maji")
	assert.Contains(t, summary, "coder")
	assert.Contains(t, summary, "api/server.py")
	assert.Contains(t, summary, "tests/test_api.py")
	assert.Contains(t, summary, "Implemented the API server")
	assert.Contains(t, summary, "2 from this run")
}

func TestBuildClosingSummaryPrioritizesCurrentRun(t *testing.T) {
	results := map[string]AgentLoopResult{
		"Akiba": {
			AgentID:  "Akiba",
			Role:     "researcher",
			Response: "Found backend comparison data.",
			Success:  true,
		},
	}

	files := []activities.WorkspaceMaterial{
		{Path: "research/old-react-comparison.md", Content: "old file content from previous run"},
		{Path: "research/akiba-backend.md", Content: "current run backend findings"},
	}

	currentRunFiles := map[string]bool{
		"research/akiba-backend.md": true,
	}
	summary := buildClosingSummary(results, files, currentRunFiles)

	// Current run file should have content
	// 本轮产出的文件带正文，历史文件只列路径，否则 closing summary 会被旧内容撑爆。
	assert.Contains(t, summary, "current run backend findings")
	// Old file should only have path, not content
	assert.Contains(t, summary, "research/old-react-comparison.md")
	assert.NotContains(t, summary, "old file content from previous run")
	assert.Contains(t, summary, "previous runs")
}

func TestBuildClosingSummaryNoFiles(t *testing.T) {
	results := map[string]AgentLoopResult{
		"Akiba": {
			AgentID:  "Akiba",
			Role:     "researcher",
			Response: "Found pricing data for AWS and Azure.",
			Success:  true,
		},
	}

	summary := buildClosingSummary(results, nil, nil)

	assert.Contains(t, summary, "1 agents completed")
	assert.Contains(t, summary, "No workspace files")
}

func TestBuildClosingSummaryTruncatesLongResponses(t *testing.T) {
	longResponse := strings.Repeat("x", 500)
	results := map[string]AgentLoopResult{
		"Akiba": {
			AgentID:  "Akiba",
			Role:     "researcher",
			Response: longResponse,
			Success:  true,
		},
	}

	summary := buildClosingSummary(results, nil, nil)
	assert.True(t, len(summary) < len(longResponse))
}

func TestIsLeadReplyValid(t *testing.T) {
	files := []activities.WorkspaceMaterial{
		{Path: "api/server.py", Content: "..."},
	}

	// Valid: long enough + references file
	assert.True(t, isLeadReplyValid(
		"Task complete. I created the API server module. Key file: api/server.py with 3 endpoints.",
		files,
	))

	// Invalid: too short
	// 过短的回复（如 "Done."）视为无效，会被要求重写。
	assert.False(t, isLeadReplyValid("Done.", files))

	// Valid: long enough reply (file path reference no longer required)
	// 现在只校验长度，不再强制要求引用文件路径。
	assert.True(t, isLeadReplyValid(
		"I completed the task and created all the necessary files for the API server module.",
		files,
	))

	// Valid: no files, just text reply
	assert.True(t, isLeadReplyValid(
		"Based on the research, AWS is 15% cheaper than Azure for compute workloads. Here are the details...",
		nil,
	))
}
