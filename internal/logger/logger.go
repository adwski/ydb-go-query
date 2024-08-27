package logger

type Logger interface {
	Error(string, ...any)
	Info(string, ...any)
	Debug(string, ...any)
	Trace(string, ...any)
}
