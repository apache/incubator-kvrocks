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
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestLogClean(t *testing.T) {
	logDir := "/tmp/kvrocks/logfile"
	require.NoError(t, os.RemoveAll(logDir))
	require.NoError(t, os.MkdirAll(logDir, os.ModePerm))

	srv := util.StartServer(t, map[string]string{
		"log-dir":            logDir,
		"log-retention-days": "0",
	})
	defer srv.Close()

	files1, err := os.ReadDir(logDir)
	require.NoError(t, err)
	if len(files1) == 0 {
		return
	}
	require.Eventually(t, func() bool {
		srv.Restart()

		files2, err := os.ReadDir(logDir)
		require.NoError(t, err)
		for _, f1 := range files1 {
			fileExists := false
			for _, f2 := range files2 {
				if f1.Name() == f2.Name() {
					fileExists = true
					break
				}
			}
			// If the file does not exist, it means the file has been cleaned
			if !fileExists {
				return true
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond)
}
