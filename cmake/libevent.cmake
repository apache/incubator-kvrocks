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

include(FetchContent)

FetchContent_Declare(libevent
  GIT_REPOSITORY https://github.com/libevent/libevent
  GIT_TAG 4c908dde58ef780eeefcc9df4db3063ca62ea862
)

set(EVENT__DISABLE_TESTS ON CACHE INTERNAL "")
set(EVENT__DISABLE_REGRESS ON CACHE INTERNAL "")
set(EVENT__DISABLE_SAMPLES ON CACHE INTERNAL "")
set(EVENT__DISABLE_OPENSSL ON CACHE INTERNAL "")
set(EVENT__LIBRARY_TYPE STATIC CACHE INTERNAL "")
FetchContent_MakeAvailable(libevent)
