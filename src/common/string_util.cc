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

#include "string_util.h"

#include <fmt/format.h>

#include <regex>
#include <string>

#include "parse_util.h"

namespace util {

std::string Float2String(double d) {
  if (std::isinf(d)) {
    return d > 0 ? "inf" : "-inf";
  }

  return fmt::format("{:.17g}", d);
}

std::string ToLower(std::string in) {
  std::transform(in.begin(), in.end(), in.begin(), [](char c) -> char { return static_cast<char>(std::tolower(c)); });
  return in;
}

bool EqualICase(std::string_view lhs, std::string_view rhs) {
  return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin(),
                                                [](char l, char r) { return std::tolower(l) == std::tolower(r); });
}

std::string Trim(std::string in, std::string_view chars) {
  if (in.empty()) return in;

  in.erase(0, in.find_first_not_of(chars));
  in.erase(in.find_last_not_of(chars) + 1);

  return in;
}

std::vector<std::string> Split(std::string_view in, std::string_view delim) {
  std::vector<std::string> out;

  if (in.empty()) {
    return out;
  }

  if (delim.empty()) {
    out.resize(in.size());
    std::transform(in.begin(), in.end(), out.begin(), [](char c) -> std::string { return {c}; });
    return out;
  }

  size_t begin = 0, end = in.find_first_of(delim);
  do {
    std::string_view elem = in.substr(begin, end - begin);
    if (!elem.empty()) out.emplace_back(elem.begin(), elem.end());
    if (end == std::string::npos) break;
    begin = end + 1;
    end = in.find_first_of(delim, begin);
  } while (true);

  return out;
}

std::vector<std::string> Split2KV(const std::string &in, const std::string &delim) {
  std::vector<std::string> out;

  std::string::size_type pos = in.find_first_of(delim);
  if (pos != std::string::npos) {
    std::string key = in.substr(0, pos);
    if (!key.empty()) out.push_back(std::move(key));

    std::string value = Trim(in.substr(pos + 1), delim);
    if (!value.empty()) out.push_back(std::move(value));
  }

  return out;
}

bool HasPrefix(const std::string &str, const std::string &prefix) {
  if (str.empty() || prefix.empty()) return false;
  return !strncasecmp(str.data(), prefix.data(), prefix.size());
}

Status ValidateGlob(std::string_view glob) {
  for (size_t idx = 0; idx < glob.size(); ++idx) {
    switch (glob[idx]) {
      case '*':
      case '?':
        break;
      case ']':
        return {Status::NotOK, "Unmatched unescaped ]"};
      case '\\':
        if (idx == glob.size() - 1) {
          return {Status::NotOK, "Trailing unescaped backslash"};
        }
        // Skip the next character: this is a literal so nothing can go wrong
        idx++;
        break;
      case '[':
        idx++;  // Skip the opening bracket
        while (idx < glob.size() && glob[idx] != ']') {
          if (glob[idx] == '\\') {
            idx += 2;
            continue;
          } else if (idx + 1 < glob.size() && glob[idx + 1] == '-') {
            if (idx + 2 >= glob.size()) {
              return {Status::NotOK, "Unterminated character range"};
            }
            // Skip the - and the end of the range
            idx += 2;
          }
          idx++;
        }
        if (idx == glob.size()) {
          return {Status::NotOK, "Unterminated [ group"};
        }
        break;
      default:
        // This is a literal: nothing can go wrong
        break;
    }
  }
  return Status::OK();
}

constexpr bool StringMatchImpl(std::string_view pattern, std::string_view string, bool ignore_case,
                               bool *skip_longer_matches, size_t recursion_depth = 0) {
  // If we want to ignore case, this is equivalent to converting both the pattern and the string to lowercase
  const auto canonicalize = [ignore_case](unsigned char c) -> unsigned char {
    return ignore_case ? static_cast<unsigned char>(std::tolower(c)) : c;
  };

  if (recursion_depth > 1000) return false;

  while (!pattern.empty() && !string.empty()) {
    switch (pattern[0]) {
      case '*':
        // Optimization: collapse multiple * into one
        while (pattern.size() >= 2 && pattern[1] == '*') {
          pattern.remove_prefix(1);
        }
        // Optimization: If the '*' is the last character in the pattern, it can match anything
        if (pattern.length() == 1) return true;
        while (!string.empty()) {
          if (StringMatchImpl(pattern.substr(1), string, ignore_case, skip_longer_matches, recursion_depth + 1))
            return true;
          if (*skip_longer_matches) return false;
          string.remove_prefix(1);
        }
        // There was no match for the rest of the pattern starting
        // from anywhere in the rest of the string. If there were
        // any '*' earlier in the pattern, we can terminate the
        // search early without trying to match them to longer
        // substrings. This is because a longer match for the
        // earlier part of the pattern would require the rest of the
        // pattern to match starting later in the string, and we
        // have just determined that there is no match for the rest
        // of the pattern starting from anywhere in the current
        // string.
        *skip_longer_matches = true;
        return false;
      case '?':
        if (string.empty()) return false;
        string.remove_prefix(1);
        break;
      case '[': {
        pattern.remove_prefix(1);
        const bool invert = pattern[0] == '^';
        if (invert) pattern.remove_prefix(1);

        bool match = false;
        while (true) {
          if (pattern.empty()) {
            // unterminated [ group: reject invalid pattern
            return false;
          } else if (pattern[0] == ']') {
            break;
          } else if (pattern.length() >= 2 && pattern[0] == '\\') {
            pattern.remove_prefix(1);
            if (pattern[0] == string[0]) match = true;
          } else if (pattern.length() >= 3 && pattern[1] == '-') {
            unsigned char start = canonicalize(pattern[0]);
            unsigned char end = canonicalize(pattern[2]);
            if (start > end) std::swap(start, end);
            const int c = canonicalize(string[0]);
            pattern.remove_prefix(2);

            if (c >= start && c <= end) match = true;
          } else if (canonicalize(pattern[0]) == canonicalize(string[0])) {
            match = true;
          }
          pattern.remove_prefix(1);
        }
        if (invert) match = !match;
        if (!match) return false;
        string.remove_prefix(1);
        break;
      }
      case '\\':
        if (pattern.length() >= 2) {
          pattern.remove_prefix(1);
        }
        [[fallthrough]];
      default:
        // Just a normal character
        if (!ignore_case) {
          if (pattern[0] != string[0]) return false;
        } else {
          if (std::tolower((int)pattern[0]) != std::tolower((int)string[0])) return false;
        }
        string.remove_prefix(1);
        break;
    }
    pattern.remove_prefix(1);
  }

  // Now that either the pattern is empty or the string is empty, this is a match iff
  // the pattern consists only of '*', and the string is empty.
  return string.empty() && std::all_of(pattern.begin(), pattern.end(), [](char c) { return c == '*'; });
}

// Given a glob [pattern] and a string [string], return true iff the string matches the glob.
// If [ignore_case] is true, the match is case-insensitive.
bool StringMatch(std::string_view glob, std::string_view str, bool ignore_case) {
  bool skip_longer_matches = false;
  return StringMatchImpl(glob, str, ignore_case, &skip_longer_matches);
}

// Split a glob pattern into a literal prefix and a suffix containing wildcards.
// For example, if the user calls [KEYS bla*bla], this function will return {"bla", "*bla"}.
// This allows the caller of this function to optimize this call by performing a
// prefix-scan on "bla" and then filtering the results using the GlobMatches function.
std::pair<std::string, std::string> SplitGlob(std::string_view glob) {
  // Stores the prefix of the glob pattern, with backslashes removed
  std::string prefix;
  // Find the first un-escaped '*', '?' or '[' character in [glob]
  for (size_t idx = 0; idx < glob.size(); ++idx) {
    if (glob[idx] == '*' || glob[idx] == '?' || glob[idx] == '[') {
      // Return a pair of views: the part of the glob before the wildcard, and the part after
      return {prefix, std::string(glob.substr(idx))};
    } else if (glob[idx] == '\\') {
      // Skip checking whether the next character is a special character
      ++idx;
      // Append the escaped special character to the prefix
      if (idx < glob.size()) prefix.push_back(glob[idx]);
    } else {
      prefix.push_back(glob[idx]);
    }
  }
  // No wildcard found, return the entire string (without the backslashes) as the prefix
  return {prefix, ""};
}

std::vector<std::string> RegexMatch(const std::string &str, const std::string &regex) {
  std::regex base_regex(regex);
  std::smatch pieces_match;
  std::vector<std::string> out;

  if (std::regex_match(str, pieces_match, base_regex)) {
    for (const auto &piece : pieces_match) {
      out.emplace_back(piece.str());
    }
  }
  return out;
}

std::string StringToHex(std::string_view input) {
  static const char hex_digits[] = "0123456789ABCDEF";
  std::string output;
  output.reserve(input.length() * 2);
  for (unsigned char c : input) {
    output.push_back(hex_digits[c >> 4]);
    output.push_back(hex_digits[c & 15]);
  }
  return output;
}

constexpr unsigned long long ExpTo1024(unsigned n) { return 1ULL << (n * 10); }

std::string BytesToHuman(uint64_t n) {
  if (n < ExpTo1024(1)) {
    return fmt::format("{}B", n);
  } else if (n < ExpTo1024(2)) {
    return fmt::format("{:.2f}K", static_cast<double>(n) / ExpTo1024(1));
  } else if (n < ExpTo1024(3)) {
    return fmt::format("{:.2f}M", static_cast<double>(n) / ExpTo1024(2));
  } else if (n < ExpTo1024(4)) {
    return fmt::format("{:.2f}G", static_cast<double>(n) / ExpTo1024(3));
  } else if (n < ExpTo1024(5)) {
    return fmt::format("{:.2f}T", static_cast<double>(n) / ExpTo1024(4));
  } else if (n < ExpTo1024(6)) {
    return fmt::format("{:.2f}P", static_cast<double>(n) / ExpTo1024(5));
  } else {
    return fmt::format("{}B", n);
  }
}

std::vector<std::string> TokenizeRedisProtocol(const std::string &value) {
  std::vector<std::string> tokens;

  if (value.empty()) {
    return tokens;
  }

  enum ParserState { stateArrayLen, stateBulkLen, stateBulkData };
  uint64_t array_len = 0, bulk_len = 0;
  int state = stateArrayLen;
  const char *start = value.data(), *end = start + value.size(), *p = nullptr;

  while (start != end) {
    switch (state) {
      case stateArrayLen: {
        if (start[0] != '*') {
          return tokens;
        }

        p = strchr(start, '\r');
        if (!p || (p == end) || p[1] != '\n') {
          tokens.clear();
          return tokens;
        }

        // parse_result expects to must be parsed successfully here
        array_len = *ParseInt<uint64_t>(std::string(start + 1, p), 10);
        start = p + 2;
        state = stateBulkLen;
        break;
      }
      case stateBulkLen: {
        if (start[0] != '$') {
          return tokens;
        }

        p = strchr(start, '\r');
        if (!p || (p == end) || p[1] != '\n') {
          tokens.clear();
          return tokens;
        }

        // parse_result expects to must be parsed successfully here
        bulk_len = *ParseInt<uint64_t>(std::string(start + 1, p), 10);
        start = p + 2;
        state = stateBulkData;
        break;
      }
      case stateBulkData: {
        if (bulk_len + 2 > static_cast<uint64_t>(end - start)) {
          tokens.clear();
          return tokens;
        }

        tokens.emplace_back(start, start + bulk_len);
        start += bulk_len + 2;
        state = stateBulkLen;
        break;
      }
    }
  }

  if (array_len != tokens.size()) {
    tokens.clear();
  }

  return tokens;
}

/* escape string where all the non-printable characters
 * (tested with isprint()) are turned into escapes in
 * the form "\n\r\a...." or "\x<hex-number>". */
std::string EscapeString(std::string_view s) {
  std::string str;
  str.reserve(s.size());

  for (auto ch : s) {
    switch (ch) {
      case '\\':
      case '"':
        str += "\\";
        str += ch;
        break;
      case '\n':
        str += "\\n";
        break;
      case '\r':
        str += "\\r";
        break;
      case '\t':
        str += "\\t";
        break;
      case '\a':
        str += "\\a";
        break;
      case '\b':
        str += "\\b";
        break;
      case '\v':
        str += "\\v";
        break;
      case '\f':
        str += "\\f";
        break;
      default:
        if (isprint(ch)) {
          str += ch;
        } else {
          str += fmt::format("\\x{:02x}", (unsigned char)ch);
        }
    }
  }

  return str;
}

std::string StringNext(std::string s) {
  for (auto iter = s.rbegin(); iter != s.rend(); ++iter) {
    if (*iter != char(0xff)) {
      (*iter)++;
      break;
    }
  }
  return s;
}

}  // namespace util
