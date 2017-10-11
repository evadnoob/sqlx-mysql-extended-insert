package logging

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Gurpartap/logrus-stack"
	"github.com/facebookgo/stack"
	"github.com/sirupsen/logrus"
)

type TextFormatter struct {
	logrus.TextFormatter
}

func (t *TextFormatter) Format(entry *logrus.Entry) ([]byte, error) {

	b := &bytes.Buffer{}

	if entry.Buffer != nil {
		b = entry.Buffer
	}

	b.WriteString(entry.Time.Format(time.RFC3339))
	b.WriteByte(' ')
	b.WriteString(strings.ToUpper(entry.Level.String()))

	b.WriteByte(' ')
	var parts = []string{"unknown"}
	if entry.Data["caller"] != nil {
		//presumes caller is added to the data from log hook
		caller := entry.Data["caller"].(stack.Frame)
		parts = strings.Split(caller.String(), "/")
	}

	//just the filename
	b.WriteString(parts[len(parts)-1])

	b.WriteByte(' ')

	b.WriteString(entry.Message)
	b.WriteByte('\n')
	return b.Bytes(), nil

}

func New() *logrus.Logger {

	level, err := logrus.ParseLevel("info")

	if err != nil {
		fmt.Println("uh oh, cannot parse log level, defaulting to info")
		level = logrus.InfoLevel
	}

	hooks := make(logrus.LevelHooks)
	hooks.Add(logrus_stack.StandardHook())

	formatter := new(TextFormatter)
	formatter.FullTimestamp = true

	return &logrus.Logger{
		Out:       os.Stdout,
		Formatter: formatter,
		Hooks:     hooks,
		Level:     level,
	}
}
