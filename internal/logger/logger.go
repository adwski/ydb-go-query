package logger

import "errors"

const (
	levelTrace = iota - 2
	levelDebug
	levelInfo
	levelError
)

var (
	ErrInvalidLevel = errors.New("invalid log level")
)

type (
	External interface {
		Error(string, []any)
		Info(string, []any)
		Debug(string, []any)
		Trace(string, []any)
	}

	Logger struct {
		ext External
		lvl int
	}
)

func parseLevel(level string) (int, error) {
	switch level {
	case "trace":
		return levelTrace, nil
	case "debug":
		return levelDebug, nil
	case "info":
		return levelInfo, nil
	case "error":
		return levelError, nil
	default:
		return 0, ErrInvalidLevel
	}
}

func New(ext External) Logger {
	return Logger{ext: ext}
}

func NewWithLevel(ext External, level string) (Logger, error) {
	lvl, err := parseLevel(level)
	if err != nil {
		return Logger{}, err
	}

	l := New(ext)
	l.lvl = lvl

	return l, nil
}

func (l *Logger) Level(lvl int) {
	l.lvl = lvl
}

func (l *Logger) Trace(msg string, args ...any) {
	if l.lvl > levelTrace {
		return
	}

	l.ext.Trace(msg, args)
}

func (l *Logger) Debug(msg string, args ...any) {
	if l.lvl > levelDebug {
		return
	}

	l.ext.Debug(msg, args)
}

func (l *Logger) Info(msg string, args ...any) {
	if l.lvl > levelInfo {
		return
	}

	l.ext.Info(msg, args)
}

func (l *Logger) Error(msg string, args ...any) {
	if l.lvl > levelError {
		return
	}

	l.ext.Error(msg, args)
}

func (l *Logger) TraceFunc(f func() (string, []any)) {
	if l.lvl > levelTrace {
		return
	}

	l.ext.Trace(f())
}

func (l *Logger) DebugFunc(f func() (string, []any)) {
	if l.lvl > levelDebug {
		return
	}

	l.ext.Debug(f())
}

func (l *Logger) InfoFunc(f func() (string, []any)) {
	if l.lvl > levelInfo {
		return
	}

	l.ext.Info(f())
}

func (l *Logger) ErrorFunc(f func() (string, []any)) {
	if l.lvl > levelError {
		return
	}

	l.ext.Error(f())
}
