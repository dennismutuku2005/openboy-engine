package auth

import (
	"testing"
	"time"
)

func TestJWTGenerateAndValidate(t *testing.T) {
	claims := Claims{
		UserID:   "usr-1",
		Username: "admin",
		Email:    "admin@openboy.dev",
		Name:     "Dennis Mutuku",
		Role:     "admin",
	}

	token, err := GenerateToken(claims)
	if err != nil {
		t.Fatalf("Failed to generate token: %v", err)
	}

	parsed, err := ValidateToken(token)
	if err != nil {
		t.Fatalf("Failed to validate token: %v", err)
	}

	if parsed.Username != "admin" || parsed.UserID != "usr-1" {
		t.Fatalf("Claims mismatch: got %+v", parsed)
	}
}

func TestJWTExpired(t *testing.T) {
	claims := Claims{
		UserID:    "usr-1",
		Username:  "admin",
		ExpiresAt: time.Now().Add(-10 * time.Minute).Unix(),
	}

	token, err := GenerateToken(claims)
	if err != nil {
		t.Fatalf("Failed to generate token: %v", err)
	}

	_, err = ValidateToken(token)
	if err != ErrExpiredToken {
		t.Fatalf("Expected ErrExpiredToken, got: %v", err)
	}
}

func TestPasswordHashing(t *testing.T) {
	pass := "secretPassword123"
	hash, salt := HashPassword(pass)

	if !CheckPassword(pass, hash, salt) {
		t.Fatalf("Password verification failed for correct password")
	}

	if CheckPassword("wrongPassword", hash, salt) {
		t.Fatalf("Password verification should have failed for wrong password")
	}
}
