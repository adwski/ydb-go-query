package zap

import (
	"fmt"

	"go.uber.org/zap"
)

type Logger struct {
	*zap.Logger
}

func NewLogger(logger *zap.Logger) *Logger {
	return &Logger{
		Logger: logger.WithOptions(zap.AddCallerSkip(1)),
	}
}

func (l *Logger) Error(msg string, fields ...any) {
	l.Logger.Error(msg, zapFields(fields...)...)
}

func (l *Logger) Info(msg string, fields ...any) {
	l.Logger.Info(msg, zapFields(fields...)...)
}

func (l *Logger) Debug(msg string, fields ...any) {
	l.Logger.Debug(msg, zapFields(fields...)...)
}

func (l *Logger) Trace(msg string, fields ...any) {
	l.Logger.Debug(msg, zapFields(fields...)...)
}

func zapFields(fields ...any) []zap.Field {
	zfs := make([]zap.Field, 0, len(fields)/2)

	for i := 0; i < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			continue
		}
		switch val := fields[i+1].(type) {
		case fmt.Stringer:
			zfs = append(zfs, zap.String(key, val.String()))
		case error:
			zfs = append(zfs, zap.String(key, val.Error()))
		default:
			zfs = append(zfs, zap.Any(key, val))
		}
	}

	return zfs
}
