package platform

import (
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var sensitiveKeys = map[string]bool{
	"jwt_signing_key": true, "MERCHANTS_DB_URI": true,
	"SHARD_A_URI": true, "SHARD_B_URI": true,
}

// NewLogger creates a structured zap logger at the specified level.
func NewLogger(level string) (*zap.Logger, error) {
	var cfg zap.Config
	if strings.EqualFold(level, "debug") {
		cfg = zap.NewDevelopmentConfig()
	} else {
		cfg = zap.NewProductionConfig()
	}

	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = zapcore.InfoLevel
	}
	cfg.Level.SetLevel(lvl)

	return cfg.Build()
}

// RedactedEnv returns os.Environ with sensitive values replaced by "***".
func RedactedEnv() []string {
	env := os.Environ()
	out := make([]string, 0, len(env))
	for _, e := range env {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) == 2 && sensitiveKeys[parts[0]] {
			out = append(out, parts[0]+"=***")
		} else {
			out = append(out, e)
		}
	}
	return out
}
