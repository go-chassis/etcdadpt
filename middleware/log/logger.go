/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package log

import (
	"fmt"

	"github.com/go-chassis/openlog"
)

var globalLogger = Logger{}

// Logger implement from grcplog.LoggerV2
type Logger struct {
	Logger openlog.Logger
}

func GetLogger() openlog.Logger {
	if globalLogger.Logger == nil {
		return openlog.GetLogger()
	}
	return globalLogger.Logger
}

func SetLogger(logger openlog.Logger) {
	globalLogger.Logger = logger
}

func (l *Logger) Info(args ...interface{}) {
	l.Logger.Info(fmt.Sprint(args...))
}

func (l *Logger) Infoln(args ...interface{}) {
	l.Logger.Info(fmt.Sprint(args...))
}

func (l *Logger) Infof(format string, args ...interface{}) {
	l.Logger.Info(fmt.Sprintf(format, args...))
}

func (l *Logger) Warning(args ...interface{}) {
	l.Logger.Warn(fmt.Sprint(args...))
}

func (l *Logger) Warningln(args ...interface{}) {
	l.Logger.Warn(fmt.Sprint(args...))
}

func (l *Logger) Warningf(format string, args ...interface{}) {
	l.Logger.Warn(fmt.Sprintf(format, args...))
}

func (l *Logger) Error(args ...interface{}) {
	l.Logger.Error(fmt.Sprint(args...), nil)
}

func (l *Logger) Errorln(args ...interface{}) {
	l.Logger.Error(fmt.Sprint(args...), nil)
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.Logger.Error(fmt.Sprintf(format, args...), nil)
}

// V reports whether verbosity level l is at least the requested verbose level.
func (l *Logger) V(_ int) bool {
	return true
}

func (l *Logger) Fatal(args ...interface{}) {
	l.Logger.Fatal(fmt.Sprint(args...), nil)
}

func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.Logger.Fatal(fmt.Sprintf(format, args...), nil)
}

func (l *Logger) Fatalln(args ...interface{}) {
	l.Logger.Fatal(fmt.Sprint(args...), nil)
}

func (l *Logger) Print(args ...interface{}) {
	l.Logger.Error(fmt.Sprint(args...), nil)
}

func (l *Logger) Printf(format string, args ...interface{}) {
	l.Logger.Error(fmt.Sprintf(format, args...), nil)
}

func (l *Logger) Println(args ...interface{}) {
	l.Logger.Error(fmt.Sprint(args...), nil)
}
