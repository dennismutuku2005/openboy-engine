package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"openboy-engine/pkg/auth"
	"openboy-engine/pkg/logger"
)

// Config represents runtime configuration loaded from .env and environment variables.
type Config struct {
	ServerPort  int
	WorkerCount int
	DataDir     string
	UIDir       string

	// MySQL Database Credentials
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string
	CustomDSN  string

	// Security
	JWTSecret string
}

// Load reads .env (if present) and environment variables into a Config instance.
func Load(envPath ...string) *Config {
	path := ".env"
	if len(envPath) > 0 && envPath[0] != "" {
		path = envPath[0]
	}

	loadDotEnv(path)

	cfg := &Config{
		ServerPort:  getEnvInt("SERVER_PORT", 4040),
		WorkerCount: getEnvInt("WORKER_COUNT", 15),
		DataDir:     getEnv("DATA_DIR", "./data"),
		UIDir:       getEnv("UI_DIR", ""),

		DBHost:     getEnv("DB_HOST", "127.0.0.1"),
		DBPort:     getEnvInt("DB_PORT", 3306),
		DBUser:     getEnv("DB_USER", "root"),
		DBPassword: getEnv("DB_PASSWORD", ""),
		DBName:     getEnv("DB_NAME", "openboy"),
		CustomDSN:  getEnv("MYSQL_DSN", ""),

		JWTSecret: getEnv("JWT_SECRET", "openboy-super-secret-jwt-signing-key-2026"),
	}

	// Apply JWT secret to auth package
	auth.SetJWTSecret(cfg.JWTSecret)

	return cfg
}

// MySQLDSN returns the formatted DSN connection string.
func (c *Config) MySQLDSN() string {
	if c.CustomDSN != "" {
		return c.CustomDSN
	}

	if c.DBHost == "" {
		return ""
	}

	// Format: username:password@tcp(host:port)/dbname?parseTime=true&charset=utf8mb4
	var authPart string
	if c.DBPassword != "" {
		authPart = fmt.Sprintf("%s:%s", c.DBUser, c.DBPassword)
	} else {
		authPart = c.DBUser
	}

	return fmt.Sprintf("%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4",
		authPart, c.DBHost, c.DBPort, c.DBName)
}

func loadDotEnv(filepath string) {
	file, err := os.Open(filepath)
	if err != nil {
		// .env is optional; ignore error if not found
		return
	}
	defer file.Close()

	logger.Info("Loaded configuration secrets from %s", filepath)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		// Strip surrounding quotes
		if len(val) >= 2 && ((val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'')) {
			val = val[1 : len(val)-1]
		}

		if os.Getenv(key) == "" {
			os.Setenv(key, val)
		}
	}
}

func getEnv(key, defaultVal string) string {
	if v, exists := os.LookupEnv(key); exists && v != "" {
		return v
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if v, exists := os.LookupEnv(key); exists && v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}
