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

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <jsoncons/json.hpp>
#include <jsoncons/json_error.hpp>
#include <jsoncons/json_options.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/cbor/cbor_encoder.hpp>
#include <jsoncons_ext/cbor/cbor_options.hpp>
#include <jsoncons_ext/jsonpath/flatten.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>
#include <jsoncons_ext/jsonpath/jsonpath_error.hpp>
#include <jsoncons_ext/mergepatch/mergepatch.hpp>
#include <limits>
#include <string>

#include "common/string_util.h"
#include "jsoncons_ext/jsonpath/jsonpath_error.hpp"
#include "server/redis_reply.h"
#include "status.h"
#include "storage/redis_metadata.h"

template <class T>
using Optionals = std::vector<std::optional<T>>;

struct JsonValue {
  enum class NumOpEnum : uint8_t {
    Incr = 1,
    Mul = 2,
  };

  static const size_t default_max_nesting_depth = 1024;

  JsonValue() = default;
  explicit JsonValue(jsoncons::basic_json<char> value) : value(std::move(value)) {}

  static StatusOr<JsonValue> FromString(std::string_view str, int max_nesting_depth = default_max_nesting_depth) {
    jsoncons::json val;

    jsoncons::json_options options;
    options.max_nesting_depth(max_nesting_depth);

    try {
      val = jsoncons::json::parse(str, options);
    } catch (const jsoncons::ser_error &e) {
      return {Status::NotOK, e.what()};
    }

    return JsonValue(std::move(val));
  }

  static StatusOr<JsonValue> FromCBOR(std::string_view str, int max_nesting_depth = default_max_nesting_depth) {
    jsoncons::json val;

    jsoncons::cbor::cbor_options options;
    options.max_nesting_depth(max_nesting_depth);

    try {
      val = jsoncons::cbor::decode_cbor<jsoncons::json>(str, options);
    } catch (const jsoncons::ser_error &e) {
      return {Status::NotOK, e.what()};
    }

    return JsonValue(std::move(val));
  }

  StatusOr<std::string> Dump(int max_nesting_depth = default_max_nesting_depth) const {
    std::string res;
    GET_OR_RET(Dump(&res, max_nesting_depth));
    return res;
  }

  Status Dump(std::string *buffer, int max_nesting_depth = default_max_nesting_depth) const {
    jsoncons::json_options options;
    options.max_nesting_depth(max_nesting_depth);

    jsoncons::compact_json_string_encoder encoder{*buffer, options};
    std::error_code ec;
    value.dump(encoder, ec);
    if (ec) {
      return {Status::NotOK, ec.message()};
    }

    return Status::OK();
  }

  StatusOr<std::string> DumpCBOR(int max_nesting_depth = default_max_nesting_depth) const {
    std::string res;
    GET_OR_RET(DumpCBOR(&res, max_nesting_depth));
    return res;
  }

  Status DumpCBOR(std::string *buffer, int max_nesting_depth = default_max_nesting_depth) const {
    jsoncons::cbor::cbor_options options;
    options.max_nesting_depth(max_nesting_depth);

    jsoncons::cbor::basic_cbor_encoder<jsoncons::string_sink<std::string>> encoder{*buffer, options};
    std::error_code ec;
    value.dump(encoder, ec);
    if (ec) {
      return {Status::NotOK, ec.message()};
    }

    return Status::OK();
  }

  StatusOr<std::string> Print(uint8_t indent_size = 0, bool spaces_after_colon = false,
                              const std::string &new_line_chars = "") const {
    std::string res;
    GET_OR_RET(Print(&res, indent_size, spaces_after_colon, new_line_chars));
    return res;
  }

  Status Print(std::string *buffer, uint8_t indent_size = 0, bool spaces_after_colon = false,
               const std::string &new_line_chars = "") const {
    jsoncons::json_options options;
    options.indent_size(indent_size);
    options.spaces_around_colon(spaces_after_colon ? jsoncons::spaces_option::space_after
                                                   : jsoncons::spaces_option::no_spaces);
    options.spaces_around_comma(jsoncons::spaces_option::no_spaces);
    options.new_line_chars(new_line_chars);

    jsoncons::json_string_encoder encoder{*buffer, options};
    std::error_code ec;
    value.dump(encoder, ec);
    if (ec) {
      return {Status::NotOK, ec.message()};
    }

    return Status::OK();
  }

  Status Set(std::string_view path, JsonValue &&new_value) {
    try {
      bool is_set = false;
      jsoncons::jsonpath::json_replace(value, path,
                                       [&new_value, &is_set](const std::string & /*path*/, jsoncons::json &origin) {
                                         origin = new_value.value;
                                         is_set = true;
                                       });

      if (!is_set) {
        // NOTE: this is a workaround since jsonpath doesn't support replace for nonexistent paths in jsoncons
        // and in this workaround we can only accept normalized path
        // refer to https://github.com/danielaparker/jsoncons/issues/496
        jsoncons::jsonpath::json_location location = jsoncons::jsonpath::json_location::parse(path);

        jsoncons::jsonpath::replace(value, location, new_value.value, true);
      }
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }

    return Status::OK();
  }

  StatusOr<Optionals<uint64_t>> StrAppend(std::string_view path, const std::string &append_value) {
    Optionals<uint64_t> results;
    try {
      std::string append_str;
      jsoncons::json append_json = jsoncons::json::parse(append_value);
      if (append_json.is_string()) {
        append_str = append_json.as_string();
      } else {
        return {Status::NotOK, "STRAPPEND need input a string to append"};
      }

      jsoncons::jsonpath::json_replace(value, path,
                                       [&append_str, &results](const std::string & /*path*/, jsoncons::json &origin) {
                                         if (origin.is_string()) {
                                           auto origin_str = origin.as_string();
                                           results.emplace_back(origin_str.length() + append_str.length());
                                           origin = origin_str + append_str;
                                         } else {
                                           results.emplace_back(std::nullopt);
                                         }
                                       });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }

    std::reverse(results.begin(), results.end());
    return results;
  }

  StatusOr<Optionals<uint64_t>> StrLen(std::string_view path) const {
    Optionals<uint64_t> results;
    try {
      jsoncons::jsonpath::json_query(value, path,
                                     [&results](const std::string & /*path*/, const jsoncons::json &origin) {
                                       if (origin.is_string()) {
                                         results.emplace_back(origin.as_string().length());
                                       } else {
                                         results.emplace_back(std::nullopt);
                                       }
                                     });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    return results;
  }

  StatusOr<std::vector<size_t>> GetBytes(std::string_view path, JsonStorageFormat format,
                                         int max_nesting_depth = default_max_nesting_depth) const {
    std::vector<size_t> results;
    Status s;
    try {
      jsoncons::jsonpath::json_query(value, path, [&](const std::string & /*path*/, const jsoncons::json &origin) {
        if (!s) return;
        std::string buffer;
        JsonValue query_value(origin);
        if (format == JsonStorageFormat::JSON) {
          s = query_value.Dump(&buffer, max_nesting_depth);
        } else if (format == JsonStorageFormat::CBOR) {
          s = query_value.DumpCBOR(&buffer, max_nesting_depth);
        }
        results.emplace_back(buffer.size());
      });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    if (!s) return s;

    return results;
  }

  StatusOr<JsonValue> Get(std::string_view path) const {
    try {
      return jsoncons::jsonpath::json_query(value, path);
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
  }

  StatusOr<Optionals<size_t>> ArrAppend(std::string_view path, const std::vector<jsoncons::json> &append_values) {
    Optionals<size_t> results;

    try {
      jsoncons::jsonpath::json_replace(
          value, path, [&append_values, &results](const std::string & /*path*/, jsoncons::json &val) {
            if (val.is_array()) {
              val.insert(val.array_range().end(), append_values.begin(), append_values.end());
              results.emplace_back(val.size());
            } else {
              results.emplace_back(std::nullopt);
            }
          });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }

    return results;
  }

  StatusOr<Optionals<uint64_t>> ArrInsert(std::string_view path, const int64_t &index,
                                          const std::vector<jsoncons::json> &insert_values) {
    Optionals<uint64_t> results;
    try {
      jsoncons::jsonpath::json_replace(
          value, path, [&insert_values, &results, index](const std::string & /*path*/, jsoncons::json &val) {
            if (val.is_array()) {
              auto len = static_cast<int64_t>(val.size());
              // When index > 0, we need index < len
              // when index < 0, we need index >= -len.
              if (index >= len || index < -len) {
                results.emplace_back(std::nullopt);
                return;
              }
              auto base_iter = index >= 0 ? val.array_range().begin() : val.array_range().end();
              val.insert(base_iter + index, insert_values.begin(), insert_values.end());
              results.emplace_back(val.size());
            } else {
              results.emplace_back(std::nullopt);
            }
          });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }

    return results;
  }

  static std::pair<ssize_t, ssize_t> NormalizeArrIndices(ssize_t start, ssize_t end, ssize_t len) {
    if (start < 0) {
      start = std::max<ssize_t>(0, len + start);
    } else {
      start = std::min<ssize_t>(start, len - 1);
    }
    if (end == 0) {
      end = len;
    } else if (end < 0) {
      end = std::max<ssize_t>(0, len + end);
    }
    end = std::min<ssize_t>(end, len);
    return {start, end};
  }

  StatusOr<Optionals<ssize_t>> ArrIndex(std::string_view path, const jsoncons::json &needle, ssize_t start,
                                        ssize_t end) const {
    Optionals<ssize_t> results;
    try {
      jsoncons::jsonpath::json_query(value, path, [&](const std::string & /*path*/, const jsoncons::json &val) {
        if (!val.is_array()) {
          results.emplace_back(std::nullopt);
          return;
        }
        auto [pstart, pend] = NormalizeArrIndices(start, end, static_cast<ssize_t>(val.size()));
        auto arr_begin = val.array_range().begin();
        auto begin_it = arr_begin + pstart;

        auto end_it = arr_begin + pend;
        auto it = std::find(begin_it, end_it, needle);
        if (it != end_it) {
          results.emplace_back(it - arr_begin);
          return;
        }
        // index not found
        results.emplace_back(-1);
      });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    return results;
  }

  StatusOr<std::vector<std::string>> Type(std::string_view path) const {
    std::vector<std::string> types;
    try {
      jsoncons::jsonpath::json_query(value, path, [&types](const std::string & /*path*/, const jsoncons::json &val) {
        switch (val.type()) {
          case jsoncons::json_type::null_value:
            types.emplace_back("null");
            break;
          case jsoncons::json_type::bool_value:
            types.emplace_back("boolean");
            break;
          case jsoncons::json_type::int64_value:
          case jsoncons::json_type::uint64_value:
            types.emplace_back("integer");
            break;
          case jsoncons::json_type::double_value:
            types.emplace_back("number");
            break;
          case jsoncons::json_type::string_value:
            types.emplace_back("string");
            break;
          case jsoncons::json_type::array_value:
            types.emplace_back("array");
            break;
          case jsoncons::json_type::object_value:
            types.emplace_back("object");
            break;
          default:
            types.emplace_back("unknown");
            break;
        }
      });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }

    return types;
  }

  StatusOr<Optionals<bool>> Toggle(std::string_view path) {
    Optionals<bool> results;
    try {
      jsoncons::jsonpath::json_replace(value, path, [&results](const std::string & /*path*/, jsoncons::json &val) {
        if (val.is_bool()) {
          val = !val.as_bool();
          results.emplace_back(val.as_bool());
        } else {
          results.emplace_back(std::nullopt);
        }
      });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    std::reverse(results.begin(), results.end());
    return results;
  }

  StatusOr<size_t> Clear(std::string_view path) {
    size_t count = 0;
    try {
      jsoncons::jsonpath::json_replace(value, path, [&count](const std::string & /*path*/, jsoncons::json &val) {
        bool is_array = val.is_array() && !val.empty();
        bool is_object = val.is_object() && !val.empty();
        bool is_number = val.is_number() && val.as<double>() != 0;

        if (is_array)
          val = jsoncons::json::array();
        else if (is_object)
          val = jsoncons::json::object();
        else if (is_number)
          val = 0;
        else
          return;

        count++;
      });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    return count;
  }

  StatusOr<Optionals<uint64_t>> ArrLen(std::string_view path) const {
    Optionals<uint64_t> results;
    try {
      jsoncons::jsonpath::json_query(value, path,
                                     [&results](const std::string & /*path*/, const jsoncons::json &basic_json) {
                                       if (basic_json.is_array()) {
                                         results.emplace_back(static_cast<uint64_t>(basic_json.size()));
                                       } else {
                                         results.emplace_back(std::nullopt);
                                       }
                                     });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    return results;
  }

  StatusOr<bool> Merge(const std::string_view path, const std::string &merge_value) {
    bool is_updated = false;
    const std::string json_root_path = "$";
    try {
      jsoncons::json patch_value = jsoncons::json::parse(merge_value);
      bool not_exists = jsoncons::jsonpath::json_query(value, path).empty();

      if (not_exists) {
        // NOTE: this is a workaround since jsonpath doesn't support replace for nonexistent paths in jsoncons
        // and in this workaround we can only accept normalized path
        // refer to https://github.com/danielaparker/jsoncons/issues/496
        jsoncons::jsonpath::json_location location = jsoncons::jsonpath::json_location::parse(path);

        jsoncons::jsonpath::replace(value, location, patch_value, true);

        is_updated = true;
      } else if (path == json_root_path) {
        // Merge with the root. Patch function complies with RFC7396 Json Merge Patch
        jsoncons::mergepatch::apply_merge_patch(value, patch_value);
        is_updated = true;
      } else if (!patch_value.is_null()) {
        // Replace value by path
        jsoncons::jsonpath::json_replace(
            value, path, [&patch_value, &is_updated](const std::string & /*path*/, jsoncons::json &target) {
              jsoncons::mergepatch::apply_merge_patch(target, patch_value);
              is_updated = true;
            });
      } else {
        // Handle null case
        jsoncons::jsonpath::remove(value, path);
        is_updated = true;
      }
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    } catch (const jsoncons::ser_error &e) {
      return {Status::NotOK, e.what()};
    }

    return is_updated;
  }

  StatusOr<Optionals<std::vector<std::string>>> ObjKeys(std::string_view path) const {
    Optionals<std::vector<std::string>> keys;
    try {
      jsoncons::jsonpath::json_query(value, path,
                                     [&keys](const std::string & /*path*/, const jsoncons::json &basic_json) {
                                       if (basic_json.is_object()) {
                                         std::vector<std::string> ret;
                                         for (const auto &member : basic_json.object_range()) {
                                           ret.push_back(member.key());
                                         }
                                         keys.emplace_back(ret);
                                       } else {
                                         keys.emplace_back(std::nullopt);
                                       }
                                     });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }

    return keys;
  }

  StatusOr<Optionals<uint64_t>> ObjLen(std::string_view path) const {
    Optionals<uint64_t> obj_lens;
    try {
      jsoncons::jsonpath::json_query(value, path,
                                     [&obj_lens](const std::string & /*path*/, const jsoncons::json &basic_json) {
                                       if (basic_json.is_object()) {
                                         obj_lens.emplace_back(static_cast<uint64_t>(basic_json.size()));
                                       } else {
                                         obj_lens.emplace_back(std::nullopt);
                                       }
                                     });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    return obj_lens;
  }

  StatusOr<Optionals<JsonValue>> ArrPop(std::string_view path, int64_t index = -1) {
    Optionals<JsonValue> popped_values;

    try {
      jsoncons::jsonpath::json_replace(value, path,
                                       [&popped_values, index](const std::string & /*path*/, jsoncons::json &val) {
                                         if (val.is_array() && !val.empty()) {
                                           auto len = static_cast<int64_t>(val.size());
                                           auto popped_iter = val.array_range().begin();
                                           if (index < 0) {
                                             popped_iter += len - std::min(len, -index);
                                           } else if (index > 0) {
                                             popped_iter += std::min(len - 1, index);
                                           }
                                           popped_values.emplace_back(*popped_iter);
                                           val.erase(popped_iter);
                                         } else {
                                           popped_values.emplace_back(std::nullopt);
                                         }
                                       });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }

    return popped_values;
  }

  StatusOr<Optionals<uint64_t>> ArrTrim(std::string_view path, int64_t start, int64_t stop) {
    Optionals<uint64_t> results;
    try {
      jsoncons::jsonpath::json_replace(
          value, path, [&results, start, stop](const std::string & /*path*/, jsoncons::json &val) {
            if (val.is_array()) {
              auto len = static_cast<int64_t>(val.size());
              auto begin_index = start < 0 ? std::max(len + start, static_cast<int64_t>(0)) : start;
              auto end_index = std::min(stop < 0 ? std::max(len + stop, static_cast<int64_t>(0)) : stop, len - 1);

              if (begin_index >= len || begin_index > end_index) {
                val = jsoncons::json::array();
                results.emplace_back(0);
                return;
              }

              auto n_val = jsoncons::json::array();
              auto begin_iter = val.array_range().begin();

              n_val.insert(n_val.end(), begin_iter + begin_index, begin_iter + end_index + 1);
              val = n_val;
              results.emplace_back(static_cast<int64_t>(n_val.size()));
            } else {
              results.emplace_back(std::nullopt);
            }
          });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    return results;
  }

  StatusOr<size_t> Del(const std::string &path) {
    size_t count = 0;
    try {
      count = jsoncons::jsonpath::remove(value, path);
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    return count;
  }

  Status NumOp(std::string_view path, const JsonValue &number, NumOpEnum op, JsonValue *result) {
    Status status = Status::OK();
    try {
      jsoncons::jsonpath::json_replace(value, path, [&](const std::string & /*path*/, jsoncons::json &origin) {
        if (!status.IsOK()) {
          return;
        }
        // is_number() will return true
        // if it's actually a string but can convert to a number
        // so here we should exclude such case
        if (!origin.is_number() || origin.is_string()) {
          result->value.push_back(jsoncons::json::null());
          return;
        }
        double v = 0;
        if (op == NumOpEnum::Incr) {
          v = origin.as_double() + number.value.as_double();
        } else if (op == NumOpEnum::Mul) {
          v = origin.as_double() * number.value.as_double();
        }
        if (std::isinf(v)) {
          status = {Status::RedisExecErr, "the result is an infinite number"};
          return;
        }
        double v_int = 0;
        if (std::modf(v, &v_int) == 0 && double(std::numeric_limits<int64_t>::min()) < v &&
            v < double(std::numeric_limits<int64_t>::max())) {
          origin = int64_t(v);
        } else {
          origin = v;
        }
        result->value.push_back(origin);
      });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    return status;
  }
  static void TransformResp(const jsoncons::json &origin, std::string &json_resp, redis::RESP resp) {
    if (origin.is_object()) {
      json_resp += redis::MultiLen(origin.size() * 2 + 1);
      json_resp += redis::SimpleString("{");

      for (const auto &json_kv : origin.object_range()) {
        json_resp += redis::BulkString(json_kv.key());
        TransformResp(json_kv.value(), json_resp, resp);
      }

    } else if (origin.is_int64() || origin.is_uint64()) {
      json_resp += redis::Integer(origin.as_integer<int64_t>());

    } else if (origin.is_string() || origin.is_double()) {
      json_resp += redis::BulkString(origin.as_string());
    } else if (origin.is_bool()) {
      json_resp += redis::SimpleString(origin.as_bool() ? "true" : "false");

    } else if (origin.is_null()) {
      json_resp += redis::NilString(resp);

    } else if (origin.is_array()) {
      json_resp += redis::MultiLen(origin.size() + 1);
      json_resp += redis::SimpleString("[");

      for (const auto &json_array_value : origin.array_range()) {
        TransformResp(json_array_value, json_resp, resp);
      }
    }
  }

  StatusOr<std::vector<std::string>> ConvertToResp(std::string_view path, redis::RESP resp) const {
    std::vector<std::string> json_resps;
    try {
      jsoncons::jsonpath::json_query(value, path, [&](const std::string & /*path*/, const jsoncons::json &origin) {
        std::string json_resp;
        TransformResp(origin, json_resp, resp);
        json_resps.emplace_back(json_resp);
      });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }

    return json_resps;
  }

  JsonValue(const JsonValue &) = default;
  JsonValue(JsonValue &&) = default;

  JsonValue &operator=(const JsonValue &) = default;
  JsonValue &operator=(JsonValue &&) = default;

  ~JsonValue() = default;

  jsoncons::json value;
};
