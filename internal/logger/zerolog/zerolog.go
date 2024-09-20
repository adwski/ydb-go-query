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

func (l *Logger) Error(msg string, fields []any) {
	emit(l.Logger.Error(), msg, fields)
}

func (l *Logger) Info(msg string, fields []any) {
	emit(l.Logger.Info(), msg, fields)
}

func (l *Logger) Debug(msg string, fields []any) {
	emit(l.Logger.Debug(), msg, fields)
}

func (l *Logger) Trace(msg string, fields []any) {
	emit(l.Logger.Trace(), msg, fields)
}

func emit(ev *zerolog.Event, msg string, fields []any) {
	if len(fields)%2 != 0 {
		fields = fields[:len(fields)-1]
	}
	for i := 0; i < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			continue
		}
		switch val := fields[i+1].(type) {
		case fmt.Stringer:
			ev = ev.Str(key, val.String())
		case error:
			ev = ev.Err(val)
		default:
			ev = ev.Any(key, val)
		}
	}
	ev.Msg(msg)
}
