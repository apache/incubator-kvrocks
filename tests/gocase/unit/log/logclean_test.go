/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package log

import (
	"os"
	"strings"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

const infoLogFileNamePart = ".INFO."

func TestInfoLogClean(t *testing.T) {
	logDir := "/tmp/kvrocks/logfile"
	require.NoError(t, os.RemoveAll(logDir))
	require.NoError(t, os.MkdirAll(logDir, os.ModePerm))

	srv := util.StartServer(t, map[string]string{
		"log-dir":            logDir,
		"log-retention-days": "-1",
	})
	srv.CloseWithoutCleanup()

	logInfoFiles := getInfoLogFilesInDir(t, logDir)
	require.NotEmpty(t, logInfoFiles)

	srv = util.StartServer(t, map[string]string{
		"log-dir":            logDir,
		"log-retention-days": "0", // all previous INFO level logs should be immediately removed
	})
	defer srv.Close()

	logInfoFiles = getInfoLogFilesInDir(t, logDir)
	require.Empty(t, logInfoFiles) // the log directory doesn't contain any INFO level log files
}

func getInfoLogFilesInDir(t *testing.T, dir string) []os.DirEntry {
	t.Helper()

	files, err := os.ReadDir(dir)
	require.NoError(t, err)

	infoLogFiles := make([]os.DirEntry, 0, len(files))

	for _, f := range files {
		if !f.Type().IsRegular() {
			continue
		}

		if !strings.Contains(f.Name(), infoLogFileNamePart) {
			continue
		}

		infoLogFiles = append(infoLogFiles, f)
	}

	return infoLogFiles
}
