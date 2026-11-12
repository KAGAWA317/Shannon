package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// JWTManager handles JWT token operations
type JWTManager struct {
	signingKey         []byte
	accessTokenExpiry  time.Duration
	refreshTokenExpiry time.Duration
	issuer             string
}

// NewJWTManager creates a new JWT manager
func NewJWTManager(signingKey string, accessExpiry, refreshExpiry time.Duration) *JWTManager {
	return &JWTManager{
		signingKey:         []byte(signingKey),
		accessTokenExpiry:  accessExpiry,
		refreshTokenExpiry: refreshExpiry,
		issuer:             "shannon-platform",
	}
}

// CustomClaims represents the custom JWT claims
type CustomClaims struct {
	jwt.RegisteredClaims
	TenantID string   `json:"tenant_id"`
	Username string   `json:"username"`
	Email    string   `json:"email"`
	Role     string   `json:"role"`
	Scopes   []string `json:"scopes"`
}

// GenerateTokenPair generates both access and refresh tokens
// GenerateTokenPair 同时生成 access token 与 refresh token
func (j *JWTManager) GenerateTokenPair(user *User) (*TokenPair, string, error) {
	// Generate access token
	accessToken, err := j.generateAccessToken(user)
	if err != nil {
		return nil, "", fmt.Errorf("failed to generate access token: %w", err)
	}

	// Generate refresh token (random string, not JWT)
	// 生成 refresh token（随机字符串，不是 JWT）
	refreshToken, refreshTokenHash, err := generateRefreshToken()
	if err != nil {
		return nil, "", fmt.Errorf("failed to generate refresh token: %w", err)
	}

	return &TokenPair{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		TokenType:    "Bearer",
		ExpiresIn:    int(j.accessTokenExpiry.Seconds()),
	}, refreshTokenHash, nil
}

// generateAccessToken creates a new JWT access token
// generateAccessToken 创建新的 JWT access token
func (j *JWTManager) generateAccessToken(user *User) (string, error) {
	now := time.Now()

	// Define scopes based on role
	scopes := j.getScopesForRole(user.Role)

	claims := CustomClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   user.ID.String(),
			Issuer:    j.issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(j.accessTokenExpiry)),
			NotBefore: jwt.NewNumericDate(now),
			ID:        uuid.New().String(),
		},
		TenantID: user.TenantID.String(),
		Username: user.Username,
		Email:    user.Email,
		Role:     user.Role,
		Scopes:   scopes,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(j.signingKey)
}

// ValidateAccessToken validates and parses a JWT access token
// ValidateAccessToken 校验并解析 JWT access token
func (j *JWTManager) ValidateAccessToken(tokenString string) (*UserContext, error) {
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (any, error) {
		// Validate signing method
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return j.signingKey, nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	if !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	claims, ok := token.Claims.(*CustomClaims)
	if !ok {
		return nil, fmt.Errorf("invalid token claims")
	}

	// Validate issuer
	if claims.Issuer != j.issuer {
		return nil, fmt.Errorf("invalid token issuer")
	}

	// Parse UUIDs
	userID, err := uuid.Parse(claims.Subject)
	if err != nil {
		return nil, fmt.Errorf("invalid user ID in token: %w", err)
	}

	tenantID, err := uuid.Parse(claims.TenantID)
	if err != nil {
		return nil, fmt.Errorf("invalid tenant ID in token: %w", err)
	}

	return &UserContext{
		UserID:    userID,
		TenantID:  tenantID,
		Username:  claims.Username,
		Email:     claims.Email,
		Role:      claims.Role,
		Scopes:    claims.Scopes,
		IsAPIKey:  false,
		TokenType: "jwt",
	}, nil
}

// RefreshAccessToken generates a new access token using a refresh token
// RefreshAccessToken 使用 refresh token 生成新的 access token
func (j *JWTManager) RefreshAccessToken(user *User) (string, error) {
	return j.generateAccessToken(user)
}

// getScopesForRole returns the default scopes for a given role
// getScopesForRole 返回指定角色的默认 scope
func (j *JWTManager) getScopesForRole(role string) []string {
	switch role {
	case RoleOwner:
		return []string{
			ScopeWorkflowsRead, ScopeWorkflowsWrite,
			ScopeAgentsExecute,
			ScopeSessionsRead, ScopeSessionsWrite,
			ScopeAPIKeysManage,
			ScopeUsersManage,
			ScopeTenantManage,
		}
	case RoleAdmin:
		return []string{
			ScopeWorkflowsRead, ScopeWorkflowsWrite,
			ScopeAgentsExecute,
			ScopeSessionsRead, ScopeSessionsWrite,
			ScopeAPIKeysManage,
			ScopeUsersManage,
		}
	default: // RoleUser
		return []string{
			ScopeWorkflowsRead, ScopeWorkflowsWrite,
			ScopeAgentsExecute,
			ScopeSessionsRead, ScopeSessionsWrite,
		}
	}
}

// generateRefreshToken creates a secure random refresh token
// generateRefreshToken 生成安全的随机 refresh token
func generateRefreshToken() (token string, hash string, err error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", fmt.Errorf("failed to generate random bytes: %w", err)
	}

	token = base64.URLEncoding.EncodeToString(b)
	hash = hashToken(token)
	return token, hash, nil
}

// hashToken creates a SHA256 hash of a token
// hashToken 计算 token 的 SHA256 哈希
func hashToken(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}

// compareTokenHash performs constant-time comparison of token hashes
// compareTokenHash 对 token 哈希做常量时间比较
func compareTokenHash(hash1, hash2 string) bool {
	return subtle.ConstantTimeCompare([]byte(hash1), []byte(hash2)) == 1
}

// ExtractBearerToken extracts the token from Authorization header
// ExtractBearerToken 从 Authorization 头中提取 token
func ExtractBearerToken(authHeader string) (string, error) {
	authHeader = strings.TrimSpace(authHeader)
	if len(authHeader) < 7 || !strings.EqualFold(authHeader[:6], "Bearer") || authHeader[6] != ' ' {
		return "", fmt.Errorf("invalid authorization header format")
	}
	token := strings.TrimSpace(authHeader[7:])
	if token == "" {
		return "", fmt.Errorf("invalid authorization header format")
	}
	return token, nil
}
