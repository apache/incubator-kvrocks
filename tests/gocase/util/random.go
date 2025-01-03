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

package util

import (
	"fmt"
	"math/rand"
	"strings"
)

func RandPath[T any](f ...func() T) T {
	index := rand.Int31n(int32(len(f)))
	return f[index]()
}

func RandPathNoResult(f ...func()) {
	index := rand.Int31n(int32(len(f)))
	f[index]()
}

// RandomSignedInt returns an integer in (-maxInt, maxInt)
func RandomSignedInt(maxInt int32) int64 {
	return rand.Int63n(int64(maxInt)*2-1) - int64(maxInt) + 1
}

// RandomInt return an integer in [0, maxInt)
func RandomInt(maxInt int64) int64 {
	return rand.Int63() % maxInt
}

func RandomIntWithSeed(maxInt, seed int64) int64 {
	r := rand.New(rand.NewSource(seed))
	return r.Int63() % maxInt
}

func RandomBool() bool {
	return RandomInt(2) != 0
}

type RandStringType int

const (
	Alpha RandStringType = iota
	Binary
)

func RandString(minInt, maxInt int, typ RandStringType) string {
	return RandStringWithSeed(minInt, maxInt, typ, rand.Int63())
}

func RandStringWithSeed(minInt, maxInt int, typ RandStringType, seed int64) string {
	r := rand.New(rand.NewSource(seed))
	length := minInt + r.Intn(maxInt-minInt+1)

	var minVal, maxVal int
	switch typ {
	case Binary:
		minVal, maxVal = 0, 255
	case Alpha:
		minVal, maxVal = 48, 122
	}

	var sb strings.Builder
	for ; length > 0; length-- {
		s := fmt.Sprintf("%c", minVal+r.Intn(maxVal-minVal+1))
		sb.WriteString(s)
	}
	return sb.String()
}

func RandomValue() string {
	return RandPath(
		// Small enough to likely collide
		func() string {
			return fmt.Sprintf("%d", RandomSignedInt(1000))
		},
		// 32 bit compressible signed/unsigned
		func() string {
			return RandPath(
				func() string {
					return fmt.Sprintf("%d", rand.Int63n(2000000000))
				},
				func() string {
					return fmt.Sprintf("%d", rand.Int63n(4000000000))
				},
			)
		},
		// 64 bit
		func() string {
			return fmt.Sprintf("%d", rand.Int63n(1000000000000))
		},
		// Random string
		func() string {
			return RandPath(
				func() string {
					return RandString(0, 256, Alpha)
				},
				func() string {
					return RandString(0, 256, Binary)
				},
			)
		},
	)
}

func RandomValueWithSeed(seed int64) string {
	return RandPath(
		// Small enough to likely collide
		func() string {
			return fmt.Sprintf("%d", RandomSignedInt(1000))
		},
		// 32 bit compressible signed/unsigned
		func() string {
			return RandPath(
				func() string {
					return fmt.Sprintf("%d", rand.Int63n(2000000000))
				},
				func() string {
					return fmt.Sprintf("%d", rand.Int63n(4000000000))
				},
			)
		},
		// 64 bit
		func() string {
			return fmt.Sprintf("%d", rand.Int63n(1000000000000))
		},
		// Random string
		func() string {
			return RandPath(
				func() string {
					return RandString(0, 256, Alpha)
				},
				func() string {
					return RandString(0, 256, Binary)
				},
			)
		},
	)
}
