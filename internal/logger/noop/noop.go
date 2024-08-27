package noop

type Logger struct {
}

func NewLogger() *Logger {
	return &Logger{}
}

func (l *Logger) Error(string, ...any) {
}

func (l *Logger) Info(string, ...any) {
}

func (l *Logger) Debug(string, ...any) {
}

func (l *Logger) Trace(string, ...any) {
}
