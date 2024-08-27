package zerolog

import (
	"fmt"

	"github.com/rs/zerolog"
)

type Logger struct {
	zerolog.Logger
}

func NewLogger(logger zerolog.Logger) *Logger {
	return &Logger{
		Logger: logger,
	}
}
func (l *Logger) Trace(msg string, fields ...any) {
	emit(l.Logger.Trace(), msg, fields...)
}

func (l *Logger) Error(msg string, fields ...any) {
	emit(l.Logger.Error(), msg, fields...)
}

func (l *Logger) Info(msg string, fields ...any) {
	emit(l.Logger.Info(), msg, fields...)
}

func (l *Logger) Debug(msg string, fields ...any) {
	emit(l.Logger.Debug(), msg, fields...)
}

func emit(ev *zerolog.Event, msg string, fields ...any) {
	for i := 0; i < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			return
		}
		switch field := fields[i+1].(type) {
		case fmt.Stringer:
			ev = ev.Any(key, field.String())
		case error:
			ev = ev.Any(key, field.Error())
		default:
			ev = ev.Any(key, field)
		}
	}
	ev.Msg(msg)
}
