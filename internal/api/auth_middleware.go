package api

import (
	"context"
	"net/http"
	"strings"

	"openboy-engine/internal/models"
	"openboy-engine/internal/store"
	"openboy-engine/pkg/auth"
)

type contextKey string

const userContextKey contextKey = "authUser"

// AuthMiddleware inspects Authorization: Bearer <token> or X-API-Key headers.
type AuthMiddleware struct {
	store *store.Store
}

// NewAuthMiddleware constructs the middleware.
func NewAuthMiddleware(s *store.Store) *AuthMiddleware {
	return &AuthMiddleware{store: s}
}

// Authenticate inspects headers and attaches the authenticated user to the context if valid.
func (am *AuthMiddleware) Authenticate(r *http.Request) (*models.User, error) {
	// 1. Try Bearer Token
	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
		tokenStr := strings.TrimSpace(authHeader[7:])
		claims, err := auth.ValidateToken(tokenStr)
		if err == nil && claims != nil {
			if user, ok := am.store.GetUserByID(claims.UserID); ok {
				return user, nil
			}
		}
	}

	// 2. Try X-API-Key header
	apiKey := r.Header.Get("X-API-Key")
	if apiKey == "" {
		apiKey = r.URL.Query().Get("api_key")
	}
	if apiKey != "" {
		if user, ok := am.store.GetUserByAPIKey(apiKey); ok {
			return user, nil
		}
	}

	return nil, auth.ErrInvalidToken
}

// GetUserFromContext retrieves the authenticated user from request context.
func GetUserFromContext(ctx context.Context) (*models.User, bool) {
	u, ok := ctx.Value(userContextKey).(*models.User)
	return u, ok && u != nil
}

// RequireAuth wraps a handler and demands valid JWT or API key.
func (am *AuthMiddleware) RequireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, err := am.Authenticate(r)
		if err != nil || user == nil {
			writeError(w, http.StatusUnauthorized, "Authentication required: valid Bearer token or X-API-Key header needed")
			return
		}

		ctx := context.WithValue(r.Context(), userContextKey, user)
		next(w, r.WithContext(ctx))
	}
}

// OptionalAuth attaches user to context if valid, but does not block unauthenticated requests.
func (am *AuthMiddleware) OptionalAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, _ := am.Authenticate(r)
		if user != nil {
			ctx := context.WithValue(r.Context(), userContextKey, user)
			r = r.WithContext(ctx)
		}
		next(w, r)
	}
}
