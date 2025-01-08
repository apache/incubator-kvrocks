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

package util_test

import (
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/assert"
)

func TestMin(t *testing.T) {
	// Arrange
	tests := []struct {
		name           string
		a              int
		b              int
		expectedResult int
	}{
		{
			name:           "a is smaller than b",
			a:              1,
			b:              2,
			expectedResult: 1,
		},
		{
			name:           "a is greater than b",
			a:              2,
			b:              1,
			expectedResult: 1,
		},
		{
			name:           "a is equal to b",
			a:              1,
			b:              1,
			expectedResult: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Act
			result := util.Min(tt.a, tt.b)

			//Assert
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestMax(t *testing.T) {
	// Arrange
	tests := []struct {
		name           string
		a              int
		b              int
		expectedResult int
	}{
		{
			name:           "a is smaller than b",
			a:              1,
			b:              2,
			expectedResult: 2,
		},
		{
			name:           "a is greater than b",
			a:              2,
			b:              1,
			expectedResult: 2,
		},
		{
			name:           "a is equal to b",
			a:              1,
			b:              1,
			expectedResult: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Act
			result := util.Max(tt.a, tt.b)

			//Assert
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}
