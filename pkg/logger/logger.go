package logger

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

var (
	mu     sync.Mutex
	output io.Writer = os.Stdout
)

const (
	colorReset   = "\033[0m"
	colorRed     = "\033[31m"
	colorGreen   = "\033[32m"
	colorYellow  = "\033[33m"
	colorBlue    = "\033[34m"
	colorMagenta = "\033[35m"
	colorCyan    = "\033[36m"
	colorGray    = "\033[90m"
)

func logf(color, tag, format string, args ...interface{}) {
	mu.Lock()
	defer mu.Unlock()

	ts := time.Now().Format("2006-01-02 15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(output, "%s[%s]%s %s[%s]%s %s\n", colorGray, ts, colorReset, color, tag, colorReset, msg)
}

func Info(format string, args ...interface{}) {
	logf(colorGreen, "INFO", format, args...)
}

func Warn(format string, args ...interface{}) {
	logf(colorYellow, "WARN", format, args...)
}

func Error(format string, args ...interface{}) {
	logf(colorRed, "ERROR", format, args...)
}

func Cron(format string, args ...interface{}) {
	logf(colorMagenta, "CRON", format, args...)
}

func Probe(format string, args ...interface{}) {
	logf(colorCyan, "PROBE", format, args...)
}

func Engine(format string, args ...interface{}) {
	logf(colorBlue, "ENGINE", format, args...)
}
