# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

include_guard()

include(cmake/utils.cmake)

FetchContent_DeclareGitHubWithMirror(xxhash
  Cyan4973/xxHash v0.8.3
  MD5=19b919a066c28a9121dd9767e01d0c71
)

FetchContent_GetProperties(xxhash)
if(NOT xxhash_POPULATED)
  FetchContent_Populate(xxhash)

  set(BUILD_SHARED_LIBS OFF)
  set(XXHASH_BUILD_XXHSUM OFF)

  add_subdirectory(${xxhash_SOURCE_DIR}/cmake_unofficial ${xxhash_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()
