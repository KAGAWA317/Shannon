package handlers

import (
	"testing"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
)

func TestValidateAgentInput_RequiredFields(t *testing.T) {
	agent := &activities.AgentDefinition{
		Name: "test-agent",
		InputSchema: map[string]any{
			"required": []any{"keywords", "country"},
			"properties": map[string]any{
				"keywords": map[string]any{
					"type": "string",
				},
				"country": map[string]any{
					"type": "string",
				},
			},
		},
	}

	tests := []struct {
		name    string
		input   map[string]any
		wantErr bool
		errMsg  string
	}{
		{
			name: "all required fields present",
			input: map[string]any{
				"keywords": "test query",
				"country":  "us",
			},
			wantErr: false,
		},
		{
			name: "missing required field keywords",
			input: map[string]any{
				"country": "us",
			},
			wantErr: true,
			errMsg:  "missing required field: keywords",
		},
		{
			name: "missing required field country",
			input: map[string]any{
				"keywords": "test",
			},
			wantErr: true,
			errMsg:  "missing required field: country",
		},
		{
			name:    "missing all required fields",
			input:   map[string]any{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAgentInput(agent, tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateAgentInput() expected error, got nil")
					return
				}
				if tt.errMsg != "" && err.Error() != "input validation failed: "+tt.errMsg {
					// Check if error contains expected message
					// 检查 error 是否包含期望的消息。
					if !contains(err.Error(), tt.errMsg) {
						t.Errorf("ValidateAgentInput() error = %v, want to contain %v", err, tt.errMsg)
					}
				}
			} else if err != nil {
				t.Errorf("ValidateAgentInput() unexpected error = %v", err)
			}
		})
	}
}

func TestValidateAgentInput_TypeValidation(t *testing.T) {
	agent := &activities.AgentDefinition{
		Name: "test-agent",
		InputSchema: map[string]any{
			"properties": map[string]any{
				"name": map[string]any{
					"type": "string",
				},
				"count": map[string]any{
					"type": "integer",
				},
				"enabled": map[string]any{
					"type": "boolean",
				},
				"tags": map[string]any{
					"type": "array",
				},
				"config": map[string]any{
					"type": "object",
				},
			},
		},
	}

	tests := []struct {
		name    string
		input   map[string]any
		wantErr bool
	}{
		{
			name:    "valid string",
			input:   map[string]any{"name": "test"},
			wantErr: false,
		},
		{
			name:    "invalid string (number)",
			input:   map[string]any{"name": 123},
			wantErr: true,
		},
		{
			name:    "valid integer",
			input:   map[string]any{"count": 10},
			wantErr: false,
		},
		{
			name:    "valid integer from float64",
			input:   map[string]any{"count": float64(10)},
			wantErr: false,
		},
		{
			name:    "invalid integer (float with decimal)",
			input:   map[string]any{"count": 10.5},
			wantErr: true,
		},
		{
			name:    "valid boolean",
			input:   map[string]any{"enabled": true},
			wantErr: false,
		},
		{
			name:    "invalid boolean (string)",
			input:   map[string]any{"enabled": "true"},
			wantErr: true,
		},
		{
			name:    "valid array",
			input:   map[string]any{"tags": []string{"a", "b"}},
			wantErr: false,
		},
		{
			name:    "invalid array (string)",
			input:   map[string]any{"tags": "not an array"},
			wantErr: true,
		},
		{
			name:    "valid object",
			input:   map[string]any{"config": map[string]any{"key": "value"}},
			wantErr: false,
		},
		{
			name:    "invalid object (string)",
			input:   map[string]any{"config": "not an object"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAgentInput(agent, tt.input)
			if tt.wantErr && err == nil {
				t.Errorf("ValidateAgentInput() expected error, got nil")
			} else if !tt.wantErr && err != nil {
				t.Errorf("ValidateAgentInput() unexpected error = %v", err)
			}
		})
	}
}

func TestValidateAgentInput_UnknownFields(t *testing.T) {
	agent := &activities.AgentDefinition{
		Name: "test-agent",
		InputSchema: map[string]any{
			"properties": map[string]any{
				"allowed_field": map[string]any{
					"type": "string",
				},
			},
		},
	}

	tests := []struct {
		name    string
		input   map[string]any
		wantErr bool
	}{
		{
			name:    "only allowed field",
			input:   map[string]any{"allowed_field": "test"},
			wantErr: false,
		},
		{
			name:    "unknown field rejected",
			input:   map[string]any{"unknown_field": "hacker input"},
			wantErr: true,
		},
		{
			name: "mixed allowed and unknown fields",
			input: map[string]any{
				"allowed_field": "test",
				"malicious":     "injection attempt",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAgentInput(agent, tt.input)
			if tt.wantErr && err == nil {
				t.Errorf("ValidateAgentInput() expected error for unknown field, got nil")
			} else if !tt.wantErr && err != nil {
				t.Errorf("ValidateAgentInput() unexpected error = %v", err)
			}
		})
	}
}

func TestValidateAgentInput_EnumValidation(t *testing.T) {
	agent := &activities.AgentDefinition{
		Name: "test-agent",
		InputSchema: map[string]any{
			"properties": map[string]any{
				"device": map[string]any{
					"type": "string",
					"enum": []any{"desktop", "mobile", "tablet"},
				},
			},
		},
	}

	tests := []struct {
		name    string
		input   map[string]any
		wantErr bool
	}{
		{
			name:    "valid enum value",
			input:   map[string]any{"device": "desktop"},
			wantErr: false,
		},
		{
			name:    "another valid enum value",
			input:   map[string]any{"device": "mobile"},
			wantErr: false,
		},
		{
			name:    "invalid enum value",
			input:   map[string]any{"device": "smartphone"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAgentInput(agent, tt.input)
			if tt.wantErr && err == nil {
				t.Errorf("ValidateAgentInput() expected error for invalid enum, got nil")
			} else if !tt.wantErr && err != nil {
				t.Errorf("ValidateAgentInput() unexpected error = %v", err)
			}
		})
	}
}

func TestValidateAgentInput_NilSchema(t *testing.T) {
	agent := &activities.AgentDefinition{
		Name:        "test-agent",
		InputSchema: nil,
	}

	// Should allow any input when no schema is defined
	// 未定义 schema 时应允许任意输入。
	err := ValidateAgentInput(agent, map[string]any{
		"any_field": "any_value",
	})
	if err != nil {
		t.Errorf("ValidateAgentInput() with nil schema should allow any input, got error = %v", err)
	}
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
