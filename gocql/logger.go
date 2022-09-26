package gocql

import "log"

type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

type stdLoggerWrapper struct {
	StdLogger
}

var Logger StdLogger = log.Default()

func (s stdLoggerWrapper) Info(v ...interface{}) {
	s.Print(v...)
}

func (s stdLoggerWrapper) Infof(format string, v ...interface{}) {
	s.Printf(format, v...)
}

func (s stdLoggerWrapper) Infoln(v ...interface{}) {
	s.Println(v...)
}

func (s stdLoggerWrapper) Warn(v ...interface{}) {
	s.Print(v...)
}

func (s stdLoggerWrapper) Warnf(format string, v ...interface{}) {
	s.Printf(format, v...)
}

func (s stdLoggerWrapper) Warnln(v ...interface{}) {
	s.Println(v...)
}
