package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"google.golang.org/api/idtoken"
)

// GoogleUser represents user info extracted from Google OAuth
// GoogleUser 表示从 Google OAuth 中提取的用户信息
type GoogleUser struct {
	Email         string
	EmailVerified bool
	Name          string
	Picture       string
	Sub           string // Google user ID 注：Google 用户 ID
}

// iOSClientID is the Google OAuth client ID for iOS public client flows.
// Set via IOS_GOOGLE_CLIENT_ID env var. Empty disables iOS OAuth.
// iOSClientID 是用于 iOS 公共客户端流程的 Google OAuth client ID，通过 IOS_GOOGLE_CLIENT_ID 环境变量设置；为空则禁用 iOS OAuth。
var iOSClientID = os.Getenv("IOS_GOOGLE_CLIENT_ID")

// GoogleOAuthVerifier handles Google OAuth token verification
// GoogleOAuthVerifier 处理 Google OAuth token 校验
type GoogleOAuthVerifier struct {
	clientID            string
	clientSecret        string
	desktopClientID     string
	desktopClientSecret string
	httpClient          *http.Client
}

// NewGoogleOAuthVerifier creates a new Google OAuth verifier
// desktopClientID and desktopClientSecret are optional - for desktop/native app OAuth flows
// NewGoogleOAuthVerifier 创建 Google OAuth 校验器
// desktopClientID 与 desktopClientSecret 可选——用于桌面/原生应用的 OAuth 流程
func NewGoogleOAuthVerifier(clientID, clientSecret, desktopClientID, desktopClientSecret string) *GoogleOAuthVerifier {
	return &GoogleOAuthVerifier{
		clientID:            clientID,
		clientSecret:        clientSecret,
		desktopClientID:     desktopClientID,
		desktopClientSecret: desktopClientSecret,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// VerifyIDToken verifies a Google ID token and extracts user info
// Used for web flow where frontend sends ID token directly
// VerifyIDToken 校验 Google ID token 并提取用户信息
// 用于前端直接发送 ID token 的 web 流程
func (v *GoogleOAuthVerifier) VerifyIDToken(ctx context.Context, idToken string) (*GoogleUser, error) {
	return v.verifyIDTokenWithAudience(ctx, idToken, v.clientID)
}

// verifyIDTokenWithAudience verifies a Google ID token against a specific client ID (audience)
// verifyIDTokenWithAudience 针对指定 client ID（audience）校验 Google ID token
func (v *GoogleOAuthVerifier) verifyIDTokenWithAudience(ctx context.Context, idToken, expectedAudience string) (*GoogleUser, error) {
	if expectedAudience == "" {
		return nil, fmt.Errorf("Google OAuth client ID not configured")
	}

	// Verify the ID token using Google's library
	// 使用 Google 官方库校验 ID token
	payload, err := idtoken.Validate(ctx, idToken, expectedAudience)
	if err != nil {
		return nil, fmt.Errorf("invalid Google ID token: %w", err)
	}

	// Extract claims with safe type assertions (avoid panics on missing fields)
	// 用安全的类型断言提取 claims（避免字段缺失时 panic）
	email, _ := payload.Claims["email"].(string)
	if email == "" {
		return nil, fmt.Errorf("email claim missing from Google token")
	}
	emailVerified, _ := payload.Claims["email_verified"].(bool)
	name, _ := payload.Claims["name"].(string)
	picture, _ := payload.Claims["picture"].(string)

	return &GoogleUser{
		Email:         email,
		EmailVerified: emailVerified,
		Name:          name,
		Picture:       picture,
		Sub:           payload.Subject,
	}, nil
}

// ExchangeAuthCode exchanges an authorization code for user info
// Used for desktop/mobile flows where we get auth code from deep link
// ExchangeAuthCode 用授权码换取用户信息
// 用于从深链接获得 auth code 的桌面/移动流程
func (v *GoogleOAuthVerifier) ExchangeAuthCode(ctx context.Context, authCode, redirectURI string) (*GoogleUser, error) {
	// Select credentials based on redirect_uri pattern
	// Desktop apps use custom schemes (shannon://) or loopback
	// iOS uses com.googleusercontent.apps.* scheme (public client - no secret)
	// 根据 redirect_uri 形态选择凭据
	// 桌面应用使用自定义 scheme（shannon://）或 loopback
	// iOS 使用 com.googleusercontent.apps.* scheme（公共客户端，无 secret）
	clientID, clientSecret, isPublicClient := v.selectCredentials(redirectURI)

	if clientID == "" {
		return nil, fmt.Errorf("Google OAuth credentials not configured for redirect_uri: %s", redirectURI)
	}

	// For confidential clients (web/desktop), require client_secret
	// 机密客户端（web/desktop）必须提供 client_secret
	if !isPublicClient && clientSecret == "" {
		return nil, fmt.Errorf("Google OAuth client secret not configured for redirect_uri: %s", redirectURI)
	}

	// Exchange authorization code for tokens
	// 用 authorization code 换取 token
	tokenURL := "https://oauth2.googleapis.com/token"
	values := url.Values{}
	values.Set("code", authCode)
	values.Set("client_id", clientID)
	values.Set("redirect_uri", redirectURI)
	values.Set("grant_type", "authorization_code")

	// Only include client_secret for confidential clients (not for iOS public client)
	// 仅机密客户端才包含 client_secret（iOS 公共客户端不带）
	if !isPublicClient && clientSecret != "" {
		values.Set("client_secret", clientSecret)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", tokenURL, strings.NewReader(values.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := v.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to exchange auth code: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("token exchange failed (status %d): %s", resp.StatusCode, body)
	}

	// Parse token response
	// 解析 token 响应
	var tokenResp struct {
		IDToken string `json:"id_token"`
	}
	if err := parseJSON(resp.Body, &tokenResp); err != nil {
		return nil, fmt.Errorf("failed to parse token response: %w", err)
	}

	// Verify the ID token using the same client ID that was used for exchange
	// 使用与换取时相同的 client ID 校验 ID token
	return v.verifyIDTokenWithAudience(ctx, tokenResp.IDToken, clientID)
}

// selectCredentials returns the appropriate client ID and secret based on redirect_uri
// Returns: clientID, clientSecret, isPublicClient
// - Desktop/native apps use custom schemes or loopback addresses
// - iOS uses com.googleusercontent.apps.* scheme (public client - no secret needed)
// selectCredentials 根据 redirect_uri 返回合适的 client ID 与 secret
// 返回 clientID、clientSecret、isPublicClient
// - 桌面/原生应用使用自定义 scheme 或 loopback 地址
// - iOS 使用 com.googleusercontent.apps.* scheme（公共客户端，无需 secret）
func (v *GoogleOAuthVerifier) selectCredentials(redirectURI string) (clientID, clientSecret string, isPublicClient bool) {
	// iOS pattern: com.googleusercontent.apps.{client_id}:/oauth2redirect/google
	// iOS is a public client - no client_secret required
	// iOS 形态：com.googleusercontent.apps.{client_id}:/oauth2redirect/google
	// iOS 是公共客户端，不需要 client_secret
	if strings.Contains(redirectURI, "googleusercontent.apps") {
		return iOSClientID, "", true
	}

	// Desktop patterns: custom schemes or loopback for native apps
	// 桌面形态：原生应用的自定义 scheme 或 loopback
	isDesktop := strings.HasPrefix(redirectURI, "shannon://") ||
		strings.HasPrefix(redirectURI, "http://localhost") ||
		strings.HasPrefix(redirectURI, "http://127.0.0.1")

	if isDesktop && v.desktopClientID != "" && v.desktopClientSecret != "" {
		return v.desktopClientID, v.desktopClientSecret, false
	}

	// Default to web credentials
	// 默认使用 web 凭据
	return v.clientID, v.clientSecret, false
}

// parseJSON is a helper to parse JSON response
// parseJSON 是解析 JSON response 的辅助函数
func parseJSON(body io.Reader, v any) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}
