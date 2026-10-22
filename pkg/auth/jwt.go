package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	// Default JWT signing secret (can be overridden via environment or CLI flag)
	jwtSecret = []byte("openboy-super-secret-jwt-signing-key-2026")

	ErrInvalidToken = errors.New("invalid or malformed token")
	ErrExpiredToken = errors.New("token has expired")
)

// SetJWTSecret overrides the signing secret.
func SetJWTSecret(secret string) {
	if secret != "" {
		jwtSecret = []byte(secret)
	}
}

// Claims represents the standard JWT payload claims.
type Claims struct {
	UserID    string `json:"sub"`
	Username  string `json:"username"`
	Email     string `json:"email"`
	Name      string `json:"name"`
	Role      string `json:"role"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
}

// GenerateToken creates a signed JWT string with a 72-hour expiration.
func GenerateToken(claims Claims) (string, error) {
	now := time.Now()
	claims.IssuedAt = now.Unix()
	if claims.ExpiresAt == 0 {
		claims.ExpiresAt = now.Add(72 * time.Hour).Unix()
	}

	header := map[string]string{
		"alg": "HS256",
		"typ": "JWT",
	}

	headerBytes, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	headerB64 := base64.RawURLEncoding.EncodeToString(headerBytes)

	claimsBytes, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	claimsB64 := base64.RawURLEncoding.EncodeToString(claimsBytes)

	data := headerB64 + "." + claimsB64
	sig := sign(data, jwtSecret)

	return data + "." + sig, nil
}

// ValidateToken parses, validates signature, and checks expiration of a JWT string.
func ValidateToken(tokenStr string) (*Claims, error) {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return nil, ErrInvalidToken
	}

	data := parts[0] + "." + parts[1]
	expectedSig := sign(data, jwtSecret)

	if !hmac.Equal([]byte(parts[2]), []byte(expectedSig)) {
		return nil, ErrInvalidToken
	}

	claimsBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, ErrInvalidToken
	}

	var claims Claims
	if err := json.Unmarshal(claimsBytes, &claims); err != nil {
		return nil, ErrInvalidToken
	}

	if time.Now().Unix() > claims.ExpiresAt {
		return nil, ErrExpiredToken
	}

	return &claims, nil
}

func sign(data string, secret []byte) string {
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(data))
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

// HashPassword hashes a plaintext password with a random salt using SHA-256.
func HashPassword(password string) (string, string) {
	saltBytes := make([]byte, 16)
	rand.Read(saltBytes)
	salt := hex.EncodeToString(saltBytes)

	hash := sha256.Sum256([]byte(password + ":" + salt))
	return hex.EncodeToString(hash[:]), salt
}

// CheckPassword verifies a plaintext password against a stored hash and salt.
func CheckPassword(password, storedHash, salt string) bool {
	hash := sha256.Sum256([]byte(password + ":" + salt))
	return hex.EncodeToString(hash[:]) == storedHash
}

// GenerateAPIKey generates a cryptographically random live API key.
func GenerateAPIKey() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return fmt.Sprintf("ob_live_%s", hex.EncodeToString(bytes))
}
