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
 *
 */

#pragma once

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "type_util.h"

class [[nodiscard]] Status {
 public:
  enum Code : unsigned char {
    NotOK = 1,
    NotFound,
    NotSupported,
    InvalidArgument,

    // DB
    DBOpenErr,
    DBBackupErr,
    DBGetWALErr,

    // Redis
    RedisUnknownCmd,
    RedisInvalidCmd,
    RedisParseErr,
    RedisExecErr,
    RedisErrorNoPrefix,
    RedisNoProto,
    RedisLoading,
    RedisMasterDown,
    RedisNoScript,
    RedisNoAuth,
    RedisWrongType,
    RedisReadOnly,
    RedisExecAbort,
    RedisBusyGroup,
    RedisNoGroup,
    RedisMoved,
    RedisCrossSlot,
    RedisTryAgain,
    RedisClusterDown,

    // Cluster
    ClusterInvalidInfo,

    // Blocking
    BlockingCmd,

    // Search
    NoPrefixMatched,
    TypeMismatched,
  };

  Status() : impl_{nullptr} {}

  Status(Code code, std::string msg = {}) : impl_{new Impl{code, std::move(msg)}} {  // NOLINT
    CHECK(code != cOK);
  }

  Status(const Status& s) : impl_{s.impl_ ? new Impl{s.impl_->code, s.impl_->msg} : nullptr} {}
  Status(Status&&) = default;

  Status& operator=(const Status& s) {
    Status tmp = s;
    return *this = std::move(tmp);
  }
  Status& operator=(Status&& s) = default;

  ~Status() = default;

  template <Code code>
  bool Is() const {
    return impl_ && impl_->code == code;
  }

  bool IsOK() const { return !impl_; }
  explicit operator bool() const { return IsOK(); }

  Code GetCode() const { return impl_ ? impl_->code : cOK; }

  std::string Msg() const& {
    if (*this) return ok_msg;
    return impl_->msg;
  }

  std::string Msg() && {
    if (*this) return ok_msg;
    return std::move(impl_->msg);
  }

  static Status OK() { return {}; }

  static Status FromErrno() { return {NotOK, strerror(errno)}; }
  static Status FromErrno(std::string_view prefix) { return {NotOK, fmt::format("{}: {}", prefix, strerror(errno))}; }

  Status Prefixed(std::string_view prefix) const {
    if (*this) {
      return {};
    }
    return {impl_->code, fmt::format("{}: {}", prefix, impl_->msg)};
  }

  void GetValue() {}

  static constexpr const char* ok_msg = "ok";

 private:
  struct Impl {
    Code code;
    std::string msg;
  };

  std::unique_ptr<Impl> impl_;

  static constexpr Code cOK = static_cast<Code>(0);  // NOLINT

  template <typename>
  friend struct StatusOr;
};

template <typename, typename = void>
struct StringInStatusOr : private std::string {
  using BaseType = std::string;
  static constexpr bool inplace = true;

  explicit StringInStatusOr(std::string&& v) : BaseType(std::move(v)) {}

  template <typename U>
  StringInStatusOr(StringInStatusOr<U>&& v) : BaseType(*std::move(v)) {}  // NOLINT
  StringInStatusOr(const StringInStatusOr& v) = delete;

  StringInStatusOr& operator=(const StringInStatusOr&) = delete;

  std::string& operator*() & { return *this; }

  const std::string& operator*() const& { return *this; }

  std::string&& operator*() && { return std::move(*this); }

  ~StringInStatusOr() = default;
};

template <typename T>
struct StringInStatusOr<T, std::enable_if_t<sizeof(T) < sizeof(std::string)>> : private std::unique_ptr<std::string> {
  using BaseType = std::unique_ptr<std::string>;
  static constexpr bool inplace = false;

  explicit StringInStatusOr(std::string&& v) : BaseType(new std::string(std::move(v))) {}

  template <typename U, typename std::enable_if_t<StringInStatusOr<U>::inplace, int> = 0>
  StringInStatusOr(StringInStatusOr<U>&& v) : BaseType(new std::string(*std::move(v))) {}  // NOLINT
  template <typename U, typename std::enable_if_t<!StringInStatusOr<U>::inplace, int> = 0>
  StringInStatusOr(StringInStatusOr<U>&& v)  // NOLINT
      : BaseType((typename StringInStatusOr<U>::BaseType &&)(std::move(v))) {}

  StringInStatusOr(const StringInStatusOr& v) = delete;

  StringInStatusOr& operator=(const StringInStatusOr&) = delete;

  std::string& operator*() & { return BaseType::operator*(); }

  const std::string& operator*() const& { return BaseType::operator*(); }

  std::string&& operator*() && { return std::move(BaseType::operator*()); }

  ~StringInStatusOr() = default;
};

template <typename>
struct StatusOr;

template <typename>
struct IsStatusOr : std::integral_constant<bool, false> {};

template <typename T>
struct IsStatusOr<StatusOr<T>> : std::integral_constant<bool, true> {};

template <typename T>
struct [[nodiscard]] StatusOr {
  static_assert(!std::is_same_v<T, Status>, "ValueType cannot be Status");
  static_assert(!std::is_same_v<T, Status::Code>, "ValueType cannot be Status::Code");
  static_assert(!IsStatusOr<T>::value, "ValueType cannot be StatusOr");
  static_assert(!std::is_reference_v<T>, "ValueType cannot be reference");

  using ValueType = T;

  // we use std::unique_ptr to make the error part as small as enough
  using ErrorType = StringInStatusOr<ValueType>;

  using Code = Status::Code;

  StatusOr(Status s) : code_(s.GetCode()) {  // NOLINT
    CHECK(!s);
    new (&error) ErrorType(std::move(s.impl_->msg));
  }

  StatusOr(Code code, std::string msg = {}) : code_(code) {  // NOLINT
    CHECK(code != Status::cOK);
    new (&error) ErrorType(std::move(msg));
  }

  template <typename... Ts,
            typename std::enable_if<(sizeof...(Ts) > 0 && !std::is_same_v<Status, RemoveCVRef<FirstElement<Ts...>>> &&
                                     !std::is_same_v<Code, RemoveCVRef<FirstElement<Ts...>>> &&
                                     !std::is_same_v<StatusOr, RemoveCVRef<FirstElement<Ts...>>>),
                                    int>::type = 0>  // NOLINT
  StatusOr(Ts&&... args) : code_(Status::cOK) {      // NOLINT
    new (&value) ValueType(std::forward<Ts>(args)...);
  }

  StatusOr(const StatusOr&) = delete;

  template <typename U, typename std::enable_if<std::is_convertible<U, T>::value, int>::type = 0>
  StatusOr(StatusOr<U>&& other) : code_(other.code_) {  // NOLINT
    if (code_ == Status::cOK) {
      new (&value) ValueType(std::move(other.value));
    } else {
      new (&error) ErrorType(std::move(other.error));
    }
  }

  template <typename U, typename std::enable_if<!std::is_convertible<U, T>::value, int>::type = 0>
  StatusOr(StatusOr<U>&& other) : code_(other.code_) {  // NOLINT
    CHECK(code_ != Status::cOK);
    new (&error) ErrorType(std::move(other.error));
  }

  StatusOr& operator=(const StatusOr&) = delete;

  template <Code code>
  bool Is() const {
    return code_ == code;
  }

  bool IsOK() const { return Is<Status::cOK>(); }
  explicit operator bool() const { return IsOK(); }

  Status ToStatus() const& {
    if (*this) return Status::OK();
    return {code_, *error};
  }

  Status ToStatus() && {
    if (*this) return Status::OK();
    return {code_, std::move(*error)};
  }

  operator Status() const& { return ToStatus(); }  // NOLINT

  operator Status() && { return std::move(*this).ToStatus(); }  // NOLINT

  Code GetCode() const { return code_; }

  ValueType& GetValue() & {
    CHECK(*this);
    return value;
  }

  ValueType&& GetValue() && {
    CHECK(*this);
    return std::move(value);
  }

  const ValueType& ValueOr(const ValueType& v) const& {
    if (IsOK()) {
      return GetValue();
    } else {
      return v;
    }
  }

  ValueType ValueOr(ValueType&& v) const& {
    if (IsOK()) {
      return GetValue();
    } else {
      return std::move(v);
    }
  }

  ValueType ValueOr(const T& v) && {
    if (IsOK()) {
      return std::move(*this).GetValue();
    } else {
      return v;
    }
  }

  ValueType&& ValueOr(T&& v) && {
    if (IsOK()) {
      return std::move(*this).GetValue();
    } else {
      return std::move(v);
    }
  }

  const ValueType& GetValue() const& {
    CHECK(*this);
    return value;
  }

  ValueType& operator*() & { return GetValue(); }

  ValueType&& operator*() && { return std::move(GetValue()); }

  const ValueType& operator*() const& { return GetValue(); }

  ValueType* operator->() { return &GetValue(); }

  const ValueType* operator->() const { return &GetValue(); }

  std::string Msg() const& {
    if (*this) return Status::ok_msg;
    return *error;
  }

  std::string Msg() && {
    if (*this) return Status::ok_msg;
    return std::move(*error);
  }

  StatusOr Prefixed(std::string_view prefix) && {
    if (*this) {
      return std::move(*this);
    }
    return {code_, fmt::format("{}: {}", prefix, std::move(*error))};
  }

  ~StatusOr() {
    if (*this) {
      value.~ValueType();
    } else {
      error.~ErrorType();
    }
  }

 private:
  Status::Code code_;
  union {
    ValueType value;
    ErrorType error;
  };

  template <typename>
  friend struct StatusOr;
};

// NOLINTNEXTLINE
#define GET_OR_RET(...)                                         \
  ({                                                            \
    auto&& status = (__VA_ARGS__);                              \
    if (!status) return std::forward<decltype(status)>(status); \
    std::forward<decltype(status)>(status);                     \
  }).GetValue()
