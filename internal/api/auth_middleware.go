package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"openboy-engine/internal/models"
	"openboy-engine/internal/store"
	"openboy-engine/pkg/auth"
)

const userContextKey = "authUser"

// AuthMiddleware inspects Authorization: Bearer <token> or X-API-Key headers.
type AuthMiddleware struct {
	store *store.Store
}

// NewAuthMiddleware constructs the middleware.
func NewAuthMiddleware(s *store.Store) *AuthMiddleware {
	return &AuthMiddleware{store: s}
}

// Authenticate inspects headers and query params and returns the authenticated user if valid.
func (am *AuthMiddleware) Authenticate(c *gin.Context) (*models.User, error) {
	// 1. Try Bearer Token
	authHeader := c.GetHeader("Authorization")
	if strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
		tokenStr := strings.TrimSpace(authHeader[7:])
		claims, err := auth.ValidateToken(tokenStr)
		if err == nil && claims != nil {
			if claims.UserID != "" {
				if user, ok := am.store.GetUserByID(claims.UserID); ok {
					return user, nil
				}
			}
			if claims.Username != "" {
				if user, ok := am.store.GetUserByUsernameOrEmail(claims.Username); ok {
					return user, nil
				}
			}
			// Cryptographically verified JWT token
			return &models.User{
				ID:       claims.UserID,
				Username: claims.Username,
				Email:    claims.Email,
				Name:     claims.Name,
				Role:     claims.Role,
			}, nil
		}
	}

	// 2. Try X-API-Key header or query parameter
	apiKey := c.GetHeader("X-API-Key")
	if apiKey == "" {
		apiKey = c.Query("api_key")
	}
	if apiKey != "" {
		if user, ok := am.store.GetUserByAPIKey(apiKey); ok {
			return user, nil
		}
	}

	return nil, auth.ErrInvalidToken
}

// GetUserFromContext retrieves the authenticated user from Gin context.
func GetUserFromContext(c *gin.Context) (*models.User, bool) {
	val, exists := c.Get(userContextKey)
	if !exists {
		return nil, false
	}
	u, ok := val.(*models.User)
	return u, ok && u != nil
}

// RequireAuth returns a Gin middleware requiring valid JWT or API key.
func (am *AuthMiddleware) RequireAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		user, err := am.Authenticate(c)
		if err != nil || user == nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "Authentication required: valid Bearer token or X-API-Key header needed",
			})
			return
		}

		c.Set(userContextKey, user)
		c.Next()
	}
}

// OptionalAuth attaches user to context if valid, but does not block unauthenticated requests.
func (am *AuthMiddleware) OptionalAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		user, _ := am.Authenticate(c)
		if user != nil {
			c.Set(userContextKey, user)
		}
		c.Next()
	}
}
