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

#include <gtest/gtest.h>

#include <initializer_list>
#include <map>
#include <string>
#include <unordered_map>

TEST(StringUtil, ToLower) {
  std::map<std::string, std::string> cases{
      {"ABC", "abc"},
      {"AbC", "abc"},
      {"abc", "abc"},
  };
  for (auto iter = cases.begin(); iter != cases.end(); iter++) {
    std::string input = iter->first;
    input = util::ToLower(input);
    ASSERT_EQ(input, iter->second);
  }
}

TEST(StringUtil, Trim) {
  std::map<std::string, std::string> cases{
      {"abc", "abc"},         {"   abc    ", "abc"}, {"\t\tabc\t\t", "abc"},  {"\t\tabc\n\n", "abc"},
      {"\n\nabc\n\n", "abc"}, {" a b", "a b"},       {"a \tb\t \n", "a \tb"},
  };
  for (auto iter = cases.begin(); iter != cases.end(); iter++) {
    std::string input = iter->first;
    std::string output = util::Trim(input, " \t\n");
    ASSERT_EQ(output, iter->second);
  }
}

TEST(StringUtil, Split) {
  std::vector<std::string> expected = {"a", "b", "c", "d"};
  std::vector<std::string> array = util::Split("a,b,c,d", ",");
  ASSERT_EQ(expected, array);
  array = util::Split("a,b,,c,d,", ",");
  ASSERT_EQ(expected, array);
  array = util::Split(",a,b,c,d,", ",");
  ASSERT_EQ(expected, array);
  array = util::Split("a     b  c  d   ", " ");
  ASSERT_EQ(expected, array);
  array = util::Split("a\tb\nc\t\nd   ", " \t\n");
  ASSERT_EQ(expected, array);

  ASSERT_EQ(util::Split("a", " "), std::vector<std::string>{"a"});
  ASSERT_EQ(util::Split("a bc", " "), (std::vector<std::string>{"a", "bc"}));
  ASSERT_EQ(util::Split("a  b c def gh ", " "), (std::vector<std::string>{"a", "b", "c", "def", "gh"}));
  ASSERT_EQ(util::Split("  hello;hi,,; one ;;; two,, ", " ,;"),
            (std::vector<std::string>{"hello", "hi", "one", "two"}));
}

TEST(StringUtil, TokenizeRedisProtocol) {
  std::vector<std::string> expected = {"this", "is", "a", "test"};
  auto array = util::TokenizeRedisProtocol("*4\r\n$4\r\nthis\r\n$2\r\nis\r\n$1\r\na\r\n$4\r\ntest\r\n");
  ASSERT_EQ(expected, array);
}

TEST(StringUtil, HasPrefix) {
  ASSERT_TRUE(util::HasPrefix("has_prefix_is_true", "has_prefix"));
  ASSERT_FALSE(util::HasPrefix("has_prefix_is_false", "_has_prefix"));
  ASSERT_TRUE(util::HasPrefix("has_prefix", "has_prefix"));
  ASSERT_FALSE(util::HasPrefix("has", "has_prefix"));
}

TEST(StringUtil, ValidateGlob) {
  const auto expect_ok = [](std::string_view glob) {
    const auto result = util::ValidateGlob(glob);
    EXPECT_TRUE(result.IsOK()) << glob << ": " << result.Msg();
  };

  const auto expect_error = [](std::string_view glob, std::string_view expected_error) {
    const auto result = util::ValidateGlob(glob);
    EXPECT_FALSE(result.IsOK());
    EXPECT_EQ(result.Msg(), expected_error) << glob;
  };

  expect_ok("a");
  expect_ok("\\*");
  expect_ok("\\?");
  expect_ok("\\[");
  expect_ok("\\]");
  expect_ok("a*");
  expect_ok("a?");
  expect_ok("[ab]");
  expect_ok("[^ab]");
  expect_ok("[a-c]");
  // Surprisingly valid: this accepts the characters {a, b, c, e, f, g, -}
  expect_ok("[a-c-e-g]");
  expect_ok("[^a-c]");
  expect_ok("[-]");
  expect_ok("[\\]]");
  expect_ok("[\\\\]");
  expect_ok("[\\?]");
  expect_ok("[\\*]");
  expect_ok("[\\[]");

  expect_error("[", "Unterminated [ group");
  expect_error("]", "Unmatched unescaped ]");
  expect_error("[a", "Unterminated [ group");
  expect_error("\\", "Trailing unescaped backslash");

  // Weird case: we open a character class, with the range 'a' to ']', but then never close it
  expect_error("[a-]", "Unterminated [ group");
  expect_ok("[a-]]");
}

TEST(StringUtil, StringMatch) {
  /* Some basic tests */
  EXPECT_TRUE(util::StringMatch("a", "a"));
  EXPECT_FALSE(util::StringMatch("a", "b"));
  EXPECT_FALSE(util::StringMatch("a", "aa"));
  EXPECT_FALSE(util::StringMatch("a", ""));
  EXPECT_TRUE(util::StringMatch("", ""));
  EXPECT_FALSE(util::StringMatch("", "a"));
  EXPECT_TRUE(util::StringMatch("*", ""));
  EXPECT_TRUE(util::StringMatch("*", "a"));

  /* Simple character class tests */
  EXPECT_TRUE(util::StringMatch("[a]", "a"));
  EXPECT_FALSE(util::StringMatch("[a]", "b"));
  EXPECT_FALSE(util::StringMatch("[^a]", "a"));
  EXPECT_TRUE(util::StringMatch("[^a]", "b"));
  EXPECT_TRUE(util::StringMatch("[ab]", "a"));
  EXPECT_TRUE(util::StringMatch("[ab]", "b"));
  EXPECT_FALSE(util::StringMatch("[ab]", "c"));
  EXPECT_TRUE(util::StringMatch("[^ab]", "c"));
  EXPECT_TRUE(util::StringMatch("[a-c]", "b"));
  EXPECT_FALSE(util::StringMatch("[a-c]", "d"));

  /* Corner cases in character class parsing */
  EXPECT_TRUE(util::StringMatch("[a-c-e-g]", "-"));
  EXPECT_FALSE(util::StringMatch("[a-c-e-g]", "d"));
  EXPECT_TRUE(util::StringMatch("[a-c-e-g]", "f"));

  /* Escaping */
  EXPECT_TRUE(util::StringMatch("\\?", "?"));
  EXPECT_FALSE(util::StringMatch("\\?", "a"));
  EXPECT_TRUE(util::StringMatch("\\*", "*"));
  EXPECT_FALSE(util::StringMatch("\\*", "a"));
  EXPECT_TRUE(util::StringMatch("\\[", "["));
  EXPECT_TRUE(util::StringMatch("\\]", "]"));
  EXPECT_TRUE(util::StringMatch("\\\\", "\\"));
  EXPECT_TRUE(util::StringMatch("[\\.]", "."));
  EXPECT_TRUE(util::StringMatch("[\\-]", "-"));
  EXPECT_TRUE(util::StringMatch("[\\[]", "["));
  EXPECT_TRUE(util::StringMatch("[\\]]", "]"));
  EXPECT_TRUE(util::StringMatch("[\\\\]", "\\"));
  EXPECT_TRUE(util::StringMatch("[\\?]", "?"));
  EXPECT_TRUE(util::StringMatch("[\\*]", "*"));

  /* Simple wild cards */
  EXPECT_TRUE(util::StringMatch("?", "a"));
  EXPECT_FALSE(util::StringMatch("?", "aa"));
  EXPECT_FALSE(util::StringMatch("??", "a"));
  EXPECT_TRUE(util::StringMatch("?x?", "axb"));
  EXPECT_FALSE(util::StringMatch("?x?", "abx"));
  EXPECT_FALSE(util::StringMatch("?x?", "xab"));

  /* Asterisk wild cards (backtracking) */
  EXPECT_FALSE(util::StringMatch("*??", "a"));
  EXPECT_TRUE(util::StringMatch("*??", "ab"));
  EXPECT_TRUE(util::StringMatch("*??", "abc"));
  EXPECT_TRUE(util::StringMatch("*??", "abcd"));
  EXPECT_FALSE(util::StringMatch("??*", "a"));
  EXPECT_TRUE(util::StringMatch("??*", "ab"));
  EXPECT_TRUE(util::StringMatch("??*", "abc"));
  EXPECT_TRUE(util::StringMatch("??*", "abcd"));
  EXPECT_FALSE(util::StringMatch("?*?", "a"));
  EXPECT_TRUE(util::StringMatch("?*?", "ab"));
  EXPECT_TRUE(util::StringMatch("?*?", "abc"));
  EXPECT_TRUE(util::StringMatch("?*?", "abcd"));
  EXPECT_TRUE(util::StringMatch("*b", "b"));
  EXPECT_TRUE(util::StringMatch("*b", "ab"));
  EXPECT_FALSE(util::StringMatch("*b", "ba"));
  EXPECT_TRUE(util::StringMatch("*b", "bb"));
  EXPECT_TRUE(util::StringMatch("*b", "abb"));
  EXPECT_TRUE(util::StringMatch("*b", "bab"));
  EXPECT_TRUE(util::StringMatch("*bc", "abbc"));
  EXPECT_TRUE(util::StringMatch("*bc", "bc"));
  EXPECT_TRUE(util::StringMatch("*bc", "bbc"));
  EXPECT_TRUE(util::StringMatch("*bc", "bcbc"));

  /* Multiple asterisks (complex backtracking) */
  EXPECT_TRUE(util::StringMatch("*ac*", "abacadaeafag"));
  EXPECT_TRUE(util::StringMatch("*ac*ae*ag*", "abacadaeafag"));
  EXPECT_TRUE(util::StringMatch("*a*b*[bc]*[ef]*g*", "abacadaeafag"));
  EXPECT_FALSE(util::StringMatch("*a*b*[ef]*[cd]*g*", "abacadaeafag"));
  EXPECT_TRUE(util::StringMatch("*abcd*", "abcabcabcabcdefg"));
  EXPECT_TRUE(util::StringMatch("*ab*cd*", "abcabcabcabcdefg"));
  EXPECT_TRUE(util::StringMatch("*abcd*abcdef*", "abcabcdabcdeabcdefg"));
  EXPECT_FALSE(util::StringMatch("*abcd*", "abcabcabcabcefg"));
  EXPECT_FALSE(util::StringMatch("*ab*cd*", "abcabcabcabcefg"));

  /* Robustness to exponential blow-ups with lots of non-collapsible asterisks */
  EXPECT_TRUE(
      util::StringMatch("?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
  EXPECT_FALSE(
      util::StringMatch("?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*b", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
}

TEST(StringUtil, SplitGlob) {
  using namespace std::string_literals;

  // Basic functionality: no escaped characters
  EXPECT_EQ(util::SplitGlob(""), std::make_pair(""s, ""s));
  EXPECT_EQ(util::SplitGlob("string"), std::make_pair("string"s, ""s));
  EXPECT_EQ(util::SplitGlob("string*"), std::make_pair("string"s, "*"s));
  EXPECT_EQ(util::SplitGlob("*string"), std::make_pair(""s, "*string"s));
  EXPECT_EQ(util::SplitGlob("str*ing"), std::make_pair("str"s, "*ing"s));
  EXPECT_EQ(util::SplitGlob("string?"), std::make_pair("string"s, "?"s));
  EXPECT_EQ(util::SplitGlob("?string"), std::make_pair(""s, "?string"s));
  EXPECT_EQ(util::SplitGlob("ab[cd]ef"), std::make_pair("ab"s, "[cd]ef"s));

  // Escaped characters; also tests that prefix is trimmed of backslashes
  EXPECT_EQ(util::SplitGlob("str\\*ing*"), std::make_pair("str*ing"s, "*"s));
  EXPECT_EQ(util::SplitGlob("str\\?ing?"), std::make_pair("str?ing"s, "?"s));
  EXPECT_EQ(util::SplitGlob("str\\[ing[a]"), std::make_pair("str[ing"s, "[a]"s));
}

TEST(StringUtil, EscapeString) {
  std::unordered_map<std::string, std::string> origin_to_escaped = {
      {"abc", "abc"},
      {"abc\r\n", "abc\\r\\n"},
      {"\a\n\1foo\r", "\\a\\n\\x01foo\\r"},
  };

  for (const auto &item : origin_to_escaped) {
    const std::string &origin = item.first;
    const std::string &escaped = item.second;
    ASSERT_TRUE(util::EscapeString(origin) == escaped);
  }
}

TEST(StringUtil, RegexMatchExtractSSTFile) {
  // Test for ExtractSSTFileNameFromError() in event_listener.cc
  auto bg_error_str = {"Corruption: Corrupt or unsupported format_version: 1005 in /tmp/kvrocks/data/db/000038.sst",
                       "Corruption: Bad table magic number: expected 9863518390377041911, found 9863518390377041912 in "
                       "/tmp/kvrocks_db/data/db/000038.sst",
                       "Corruption: block checksum mismatch: stored = 3308200672, computed = 51173877, type = 4  in "
                       "/tmp/kvrocks_db/data/db/000038.sst offset 0 size 15715"};

  for (const auto &str : bg_error_str) {
    auto match_results = util::RegexMatch(str, ".*(/\\w*\\.sst).*");
    ASSERT_TRUE(match_results.size() == 2);
    ASSERT_TRUE(match_results[1] == "/000038.sst");
  }
}
