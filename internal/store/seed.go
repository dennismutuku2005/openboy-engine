package store

import (
	"time"

	"openboy-engine/internal/models"
	"openboy-engine/pkg/auth"
)

func getDefaultUsers() []models.User {
	now := time.Now()
	passHash, salt := auth.HashPassword("admin")

	return []models.User{
		{
			ID:           "usr-1",
			Username:     "admin",
			Email:        "admin@openboy.dev",
			PasswordHash: passHash,
			PasswordSalt: salt,
			Name:         "Dennis Mutuku",
			Role:         "admin",
			APIKey:       "ob_live_8f99e3a07c1264b91",
			CreatedAt:    now.Add(-72 * time.Hour),
			UpdatedAt:    now,
		},
		{
			ID:           "usr-2",
			Username:     "operator",
			Email:        "ops@openboy.dev",
			PasswordHash: passHash,
			PasswordSalt: salt,
			Name:         "Lead DevOps Engineer",
			Role:         "operator",
			APIKey:       "ob_live_1a2b3c4d5e6f7g8h9",
			CreatedAt:    now.Add(-48 * time.Hour),
			UpdatedAt:    now,
		},
	}
}

func getDefaultMonitors() []models.Monitor {
	return []models.Monitor{}
}

func getDefaultBatchers() []models.Batcher {
	return []models.Batcher{}
}

func getDefaultIncidents() []models.Incident {
	return []models.Incident{}
}

func getDefaultLogs() []models.ExecutionLog {
	return []models.ExecutionLog{}
}
