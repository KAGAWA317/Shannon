package auth

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// Sentinel errors for structured error handling across package boundaries.
// 跨包结构化错误处理用的哨兵错误。
var (
	// ErrTenantNotFound is returned by ProvisionUser when the target tenant does not exist or is inactive.
	// 目标租户不存在或未激活时，ProvisionUser 返回 ErrTenantNotFound。
	ErrTenantNotFound = errors.New("tenant not found or inactive")
	// ErrUsernameTakenByOtherTenant is returned when the username is already registered under a different tenant.
	// username 已在其它租户下注册时返回 ErrUsernameTakenByOtherTenant。
	ErrUsernameTakenByOtherTenant = errors.New("username already registered under a different tenant")
)

// JSONMap handles JSON database columns
// JSONMap 处理 JSON 数据库列
type JSONMap map[string]any

// Scan implements sql.Scanner interface
// Scan 实现 sql.Scanner 接口
func (j *JSONMap) Scan(value any) error {
	if value == nil {
		*j = make(map[string]any)
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return nil
	}
	return json.Unmarshal(bytes, j)
}

// Value implements driver.Valuer interface
// Value 实现 driver.Valuer 接口
func (j JSONMap) Value() (driver.Value, error) {
	if j == nil {
		return "{}", nil
	}
	return json.Marshal(j)
}

// User represents an authenticated user
// User 表示一个已认证用户
type User struct {
	ID              uuid.UUID  `json:"id" db:"id"`
	Email           string     `json:"email" db:"email"`
	Username        string     `json:"username" db:"username"`
	PasswordHash    string     `json:"-" db:"password_hash"`
	FullName        *string    `json:"full_name,omitempty" db:"full_name"` // Nullable in DB 注：数据库中可为 NULL
	TenantID        uuid.UUID  `json:"tenant_id" db:"tenant_id"`
	Role            string     `json:"role" db:"role"` // user, admin, owner 注：取值 user、admin、owner
	IsActive        bool       `json:"is_active" db:"is_active"`
	IsVerified      bool       `json:"is_verified" db:"is_verified"`
	EmailVerifiedAt *time.Time `json:"email_verified_at,omitempty" db:"email_verified_at"`
	CreatedAt       time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at" db:"updated_at"`
	LastLogin       *time.Time `json:"last_login,omitempty" db:"last_login"`
	Metadata        JSONMap    `json:"metadata,omitempty" db:"metadata"`
}

// Tenant represents an organization/workspace
// Tenant 表示一个组织/工作空间
type Tenant struct {
	ID                     uuid.UUID `json:"id" db:"id"`
	Name                   string    `json:"name" db:"name"`
	Slug                   string    `json:"slug" db:"slug"`
	Plan                   string    `json:"plan" db:"plan"` // free, pro, enterprise 注：取值 free、pro、enterprise
	TokenLimit             int       `json:"token_limit" db:"token_limit"`
	MonthlyTokenUsage      int       `json:"monthly_token_usage" db:"monthly_token_usage"`
	RateLimitPerHour       int       `json:"rate_limit_per_hour" db:"rate_limit_per_hour"`
	WorkspaceRetentionDays int       `json:"workspace_retention_days,omitempty" db:"workspace_retention_days"`
	WorkspaceMaxSizeGB     int       `json:"workspace_max_size_gb,omitempty" db:"workspace_max_size_gb"`
	IsActive               bool      `json:"is_active" db:"is_active"`
	CreatedAt              time.Time `json:"created_at" db:"created_at"`
	UpdatedAt              time.Time `json:"updated_at" db:"updated_at"`
	Metadata               JSONMap   `json:"metadata,omitempty" db:"metadata"`
}

// APIKey represents an API key for programmatic access
// APIKey 表示用于编程访问的 API key
type APIKey struct {
	ID               uuid.UUID      `json:"id" db:"id"`
	KeyHash          string         `json:"-" db:"key_hash"`
	KeyPrefix        string         `json:"key_prefix" db:"key_prefix"`
	UserID           uuid.UUID      `json:"user_id" db:"user_id"`
	TenantID         uuid.UUID      `json:"tenant_id" db:"tenant_id"`
	Name             string         `json:"name" db:"name"`
	Description      *string        `json:"description,omitempty" db:"description"` // Nullable in DB 注：数据库中可为 NULL
	Scopes           pq.StringArray `json:"scopes" db:"scopes"`
	RateLimitPerHour int            `json:"rate_limit_per_hour" db:"rate_limit_per_hour"`
	LastUsed         *time.Time     `json:"last_used,omitempty" db:"last_used"`
	ExpiresAt        *time.Time     `json:"expires_at,omitempty" db:"expires_at"`
	IsActive         bool           `json:"is_active" db:"is_active"`
	CreatedAt        time.Time      `json:"created_at" db:"created_at"`
}

// RefreshToken represents an opaque refresh token (random string, not JWT)
// RefreshToken 表示不透明的 refresh token（随机字符串，不是 JWT）
type RefreshToken struct {
	ID        uuid.UUID  `json:"id" db:"id"`
	TokenHash string     `json:"-" db:"token_hash"`
	UserID    uuid.UUID  `json:"user_id" db:"user_id"`
	TenantID  uuid.UUID  `json:"tenant_id" db:"tenant_id"`
	ExpiresAt time.Time  `json:"expires_at" db:"expires_at"`
	Revoked   bool       `json:"revoked" db:"revoked"`
	RevokedAt *time.Time `json:"revoked_at,omitempty" db:"revoked_at"`
	IPAddress string     `json:"ip_address" db:"ip_address"`
	UserAgent string     `json:"user_agent" db:"user_agent"`
	CreatedAt time.Time  `json:"created_at" db:"created_at"`
}

// UserContext represents the authenticated context for a request
// UserContext 表示 request 的 authenticated context
type UserContext struct {
	UserID    uuid.UUID `json:"user_id"`
	TenantID  uuid.UUID `json:"tenant_id"`
	Username  string    `json:"username"`
	Email     string    `json:"email"`
	Role      string    `json:"role"`
	Scopes    []string  `json:"scopes"`
	IsAPIKey  bool      `json:"is_api_key"`
	TokenType string    `json:"token_type"` // jwt or api_key 注：取值 jwt 或 api_key

	// API key details
// API key 详情
	APIKeyID   uuid.UUID `json:"api_key_id,omitempty"`   // ID of the API key (if IsAPIKey) 注：API key 的 ID（当 IsAPIKey 为 true）
	APIKeyTier string    `json:"api_key_tier,omitempty"` // Tier: free, pro, enterprise 注：档位：free、pro、enterprise
	TenantPlan string    `json:"tenant_plan,omitempty"`  // Tenant plan for rate limiting 注：租户套餐，用于限流

	// Dev mode: Auth bypassed (GATEWAY_SKIP_AUTH=1)
	// dev 模式：跳过鉴权（GATEWAY_SKIP_AUTH=1）
	AuthBypassed bool `json:"auth_bypassed,omitempty"`
}

// TokenPair represents access and refresh tokens
// TokenPair 表示 access token 与 refresh token
type TokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"` // seconds 注：单位秒
}

// OAuthLoginResult represents the result of OAuth login/registration
// Separates session tokens (for web UI) from API keys (for SDK/programmatic access)
// OAuthLoginResult 表示 OAuth 登录/注册的结果
// 将 session token（web UI 用）与 API key（SDK/编程访问用）分开
type OAuthLoginResult struct {
	Tokens    *TokenPair `json:"tokens"`            // JWT access + refresh tokens for web session 注：web session 用的 JWT access + refresh token
	User      *User      `json:"user"`              // User info 注：用户信息
	APIKey    string     `json:"api_key,omitempty"` // Only set for NEW registrations 注：仅在 NEW 注册时返回
	IsNewUser bool       `json:"is_new_user"`       // True if user was just created 注：用户刚创建时为 true
}

// LoginRequest represents a login request
// LoginRequest 表示登录请求
type LoginRequest struct {
	Email    string `json:"email" validate:"required,email"`
	Password string `json:"password" validate:"required,min=8"`
}

// RegisterRequest represents a registration request
// RegisterRequest 表示注册请求
type RegisterRequest struct {
	Email    string `json:"email" validate:"required,email"`
	Username string `json:"username" validate:"required,min=3,max=50"`
	Password string `json:"password" validate:"required,min=8"`
	FullName string `json:"full_name" validate:"required"`
	TenantID string `json:"tenant_id,omitempty"` // Optional, create new if not provided 注：可选，未提供则新建
}

// CreateAPIKeyRequest represents a request to create an API key
// CreateAPIKeyRequest 表示创建 API key 的 request
type CreateAPIKeyRequest struct {
	Name        string     `json:"name" validate:"required"`
	Description string     `json:"description"`
	Scopes      []string   `json:"scopes"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
}

// AuditEvent types
// AuditEvent 类型
const (
	AuditEventLogin            = "login"
	AuditEventLogout           = "logout"
	AuditEventLoginFailed      = "login_failed"
	AuditEventTokenRefresh     = "token_refresh"
	AuditEventAPIKeyCreated    = "api_key_created"
	AuditEventAPIKeyDeleted    = "api_key_deleted"
	AuditEventAPIKeyUsed       = "api_key_used"
	AuditEventPermissionChange = "permission_changed"
	AuditEventPasswordChange   = "password_changed"
	AuditEventAccountCreated   = "account_created"
	AuditEventAccountDeleted   = "account_deleted"
)

// Scopes for authorization
// 鉴权用的 scope
const (
	ScopeWorkflowsRead   = "workflows:read"
	ScopeWorkflowsWrite  = "workflows:write"
	ScopeAgentsExecute   = "agents:execute"
	ScopeSessionsRead    = "sessions:read"
	ScopeSessionsWrite   = "sessions:write"
	ScopeAPIKeysManage   = "api_keys:manage"
	ScopeUsersManage     = "users:manage"
	ScopeTenantManage    = "tenant:manage"
	ScopeSkillsDangerous = "skills:dangerous" // Required for invoking dangerous skills 注：调用 dangerous skill 所必需
)

// User roles
// 用户角色
const (
	RoleUser  = "user"
	RoleAdmin = "admin"
	RoleOwner = "owner"
)

// Tenant plans
// 租户套餐
const (
	PlanFree       = "free"
	PlanPro        = "pro"
	PlanMax        = "max"
	PlanEnterprise = "enterprise"
)
