package log

import (
	"log"
	"os"
)

type Logger interface {
	Info(v ...any)
	Infof(format string, v ...any)
	Infoln(v ...any)

	Warn(v ...any)
	Warnf(format string, v ...any)
	Warnln(v ...any)
}

// DefaultLogger only logs warnings and critical errors.
type DefaultLogger struct {
	warn *log.Logger
}

func NewDefaultLogger() *DefaultLogger {
	res := &DefaultLogger{
		warn: log.New(os.Stderr, "WARNING ", log.LstdFlags),
	}
	return res
}

func (logger *DefaultLogger) Info(v ...any)                 {}
func (logger *DefaultLogger) Infof(format string, v ...any) {}
func (logger *DefaultLogger) Infoln(v ...any)               {}

func (logger *DefaultLogger) Warn(v ...any)                 { logger.warn.Print(v...) }
func (logger *DefaultLogger) Warnf(format string, v ...any) { logger.warn.Printf(format, v...) }
func (logger *DefaultLogger) Warnln(v ...any)               { logger.warn.Println(v...) }

// DebugLogger logs both warnings and information about important events in driver's runtime.
type DebugLogger struct {
	info *log.Logger
	warn *log.Logger
}

func NewDebugLogger() *DebugLogger {
	res := &DebugLogger{
		info: log.New(os.Stderr, "INFO ", log.LstdFlags),
		warn: log.New(os.Stderr, "WARNING ", log.LstdFlags),
	}
	return res
}

func (logger *DebugLogger) Info(v ...any)                 { logger.info.Print(v...) }
func (logger *DebugLogger) Infof(format string, v ...any) { logger.info.Printf(format, v...) }
func (logger *DebugLogger) Infoln(v ...any)               { logger.info.Println(v...) }

func (logger *DebugLogger) Warn(v ...any)                 { logger.warn.Print(v...) }
func (logger *DebugLogger) Warnf(format string, v ...any) { logger.warn.Printf(format, v...) }
func (logger *DebugLogger) Warnln(v ...any)               { logger.warn.Println(v...) }

// NopLogger doesn't log anything.
type NopLogger struct{}

func (NopLogger) Info(v ...any)                 {}
func (NopLogger) Infof(format string, v ...any) {}
func (NopLogger) Infoln(v ...any)               {}

func (NopLogger) Warn(v ...any)                 {}
func (NopLogger) Warnf(format string, v ...any) {}
func (NopLogger) Warnln(v ...any)               {}
