package config

import (
	"os"
)

type Config struct {
	HTTPAddr  string
	RedisAddr string
	RedisDB   int
}

func Load() Config {
	return Config{
		HTTPAddr:  getEnv("HTTP_ADDR", ":8080"),
		RedisAddr: getEnv("REDIS_ADDR", "localhost:6379"),
		RedisDB:   0,
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
