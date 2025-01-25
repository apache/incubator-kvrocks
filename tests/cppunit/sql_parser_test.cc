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

#include <gtest/gtest.h>
#include <search/sql_transformer.h>

#include "search/common_transformer.h"
#include "search/value.h"
#include "tao/pegtl/string_input.hpp"

using namespace kqir::sql;

static auto Parse(const std::string& in, const kqir::ParamMap& pm = {}) {
  return ParseToIR(string_input(in, "test"), pm);
}

#define AssertSyntaxError(node) ASSERT_EQ(node.Msg(), "invalid syntax");  // NOLINT

// NOLINTNEXTLINE
#define AssertIR(node, val)              \
  ASSERT_EQ(node.Msg(), Status::ok_msg); \
  ASSERT_EQ(node.GetValue()->Dump(), val);

TEST(SQLParserTest, Simple) {
  AssertSyntaxError(Parse("x"));
  AssertSyntaxError(Parse("1"));
  AssertSyntaxError(Parse("select"));
  AssertSyntaxError(Parse("where"));
  AssertSyntaxError(Parse("limit"));
  AssertSyntaxError(Parse("from a"));
  AssertSyntaxError(Parse("select 0 from"));
  AssertSyntaxError(Parse("select 0 from b"));
  AssertSyntaxError(Parse("select a from 123"));
  AssertSyntaxError(Parse("select a from \"b\""));
  AssertSyntaxError(Parse("select a from b, c"));
  AssertSyntaxError(Parse("select a from b where"));
  AssertSyntaxError(Parse("select a from b hello"));
  AssertSyntaxError(Parse("select a from b where"));
  AssertSyntaxError(Parse("select a from b where 1"));
  AssertSyntaxError(Parse("select a from b where 0"));
  AssertSyntaxError(Parse("select a from b where \"x\""));
  AssertSyntaxError(Parse("select a from b where limit 10"));
  AssertSyntaxError(Parse("select a from b where true and"));
  AssertSyntaxError(Parse("select a from b where (true"));
  AssertSyntaxError(Parse("select a from b where (true))"));
  AssertSyntaxError(Parse("select a from b where 1 >"));
  AssertSyntaxError(Parse("select a from b where x ="));
  AssertSyntaxError(Parse("select a from b where x hastag"));
  AssertSyntaxError(Parse("select a from b where ="));
  AssertSyntaxError(Parse("select a from b where hastag x"));
  AssertSyntaxError(Parse("select a from b where = 1"));
  AssertSyntaxError(Parse("select a from b where x hashtag \""));
  AssertSyntaxError(Parse(R"(select a from b where x hashtag "\p")"));
  AssertSyntaxError(Parse(R"(select a from b where x hashtag "\u11")"));
  AssertSyntaxError(Parse(R"(select a from b where x hashtag "\")"));
  AssertSyntaxError(Parse(R"(select a from b where x hashtag "abc)"));
  AssertSyntaxError(Parse("select a from b where limit 10"));
  AssertSyntaxError(Parse("select a from b limit 1, 1, 1"));
  AssertSyntaxError(Parse("select a from b limit -10"));
  AssertSyntaxError(Parse("select a from b limit"));
  AssertSyntaxError(Parse("select a from b order"));
  AssertSyntaxError(Parse("select a from b order by"));
  AssertSyntaxError(Parse("select a from b order by a bsc"));
  AssertSyntaxError(Parse("select a from b order a"));
  AssertSyntaxError(Parse("select a from b order asc"));
  AssertSyntaxError(Parse("select a from b order by a limit"));

  AssertIR(Parse("select a from b"), "select a from b where true");
  AssertIR(Parse(" select  a  from  b "), "select a from b where true");
  AssertIR(Parse("\nselect\n  a\t \tfrom \n\nb "), "select a from b where true");
  AssertIR(Parse("select * from b"), "select * from b where true");
  AssertIR(Parse("select a, b from c"), "select a, b from c where true");
  AssertIR(Parse("select a, b, c from d"), "select a, b, c from d where true");
  AssertIR(Parse("select  xY_z12_3 ,  X00  from  b"), "select xY_z12_3, X00 from b where true");
  AssertIR(Parse("select a from b where true"), "select a from b where true");
  AssertIR(Parse("select a from b where false"), "select a from b where false");
  AssertIR(Parse("select a from b where true and true"), "select a from b where (and true, true)");
  AssertIR(Parse("select a from b where false and true and false"), "select a from b where (and false, true, false)");
  AssertIR(Parse("select a from b where false or true"), "select a from b where (or false, true)");
  AssertIR(Parse("select a from b where true or false or true"), "select a from b where (or true, false, true)");
  AssertIR(Parse("select a from b where false and true or false"),
           "select a from b where (or (and false, true), false)");
  AssertIR(Parse("select a from b where false or true and false"),
           "select a from b where (or false, (and true, false))");
  AssertIR(Parse("select a from b where false and (true or false)"),
           "select a from b where (and false, (or true, false))");
  AssertIR(Parse("select a from b where (false or true) and false"),
           "select a from b where (and (or false, true), false)");
  AssertIR(Parse("select a from b where (false)"), "select a from b where false");
  AssertIR(Parse("select a from b where ((false))"), "select a from b where false");
  AssertIR(Parse("select a from b where (((false)))"), "select a from b where false");
  AssertIR(Parse("select a from b where ((false) and (false))"), "select a from b where (and false, false)");
  AssertIR(Parse("select a from b where x=1"), "select a from b where x = 1");
  AssertIR(Parse("select a from b where x = 1.0"), "select a from b where x = 1");
  AssertIR(Parse("select a from b where x = -1.234e5"), "select a from b where x = -123400");
  AssertIR(Parse("select a from b where x = -1.234e-5"), "select a from b where x = -1.234e-05");
  AssertIR(Parse("select a from b where x = 222e+5"), "select a from b where x = 22200000");
  AssertIR(Parse("select a from b where 1 = x"), "select a from b where x = 1");
  AssertIR(Parse("select a from b where 2 < y"), "select a from b where y > 2");
  AssertIR(Parse("select a from b where y > 2"), "select a from b where y > 2");
  AssertIR(Parse("select a from b where 3 >= z"), "select a from b where z <= 3");
  AssertIR(Parse("select a from b where x hastag \"hi\""), "select a from b where x hastag \"hi\"");
  AssertIR(Parse("select a from b where x tagged \"hi\""), "select a from b where x hastag \"hi\"");
  AssertIR(Parse(R"(select a from b where x hastag "a\nb")"), R"(select a from b where x hastag "a\nb")");
  AssertIR(Parse(R"(select a from b where x hastag "")"), R"(select a from b where x hastag "")");
  AssertIR(Parse(R"(select a from b where x hastag "hello ,  hi")"), R"(select a from b where x hastag "hello ,  hi")");
  AssertIR(Parse(R"(select a from b where x hastag "a\nb\t\n")"), R"(select a from b where x hastag "a\nb\t\n")");
  AssertIR(Parse(R"(select a from b where x hastag "a\u0000")"), R"(select a from b where x hastag "a\x00")");
  AssertIR(Parse("select a from b where x > 1 and y < 33"), "select a from b where (and x > 1, y < 33)");
  AssertIR(Parse("select a from b where x >= 1 and y hastag \"hi\" or c <= 233"),
           "select a from b where (or (and x >= 1, y hastag \"hi\"), c <= 233)");
  AssertIR(Parse("select a from b limit 10"), "select a from b where true limit 0, 10");
  AssertIR(Parse("select a from b limit 2, 3"), "select a from b where true limit 2, 3");
  AssertIR(Parse("select a from b order by a"), "select a from b where true sortby a, asc");
  AssertIR(Parse("select a from b order by c desc"), "select a from b where true sortby c, desc");
  AssertIR(Parse("select a from b order by c desc limit 10"), "select a from b where true sortby c, desc limit 0, 10");
  AssertIR(Parse("select a from b order by a limit 10"), "select a from b where true sortby a, asc limit 0, 10");
  AssertIR(Parse("select a from b where c = 1 limit 10"), "select a from b where c = 1 limit 0, 10");
  AssertIR(Parse("select a from b where c = 1 and d hastag \"x\" order by e"),
           "select a from b where (and c = 1, d hastag \"x\") sortby e, asc");
  AssertIR(Parse("select a from b where c = 1 or d hastag \"x\" and 2 <= e order by e asc limit 0, 10"),
           "select a from b where (or c = 1, (and d hastag \"x\", e >= 2)) sortby e, asc limit 0, 10");
}

TEST(SQLParserTest, Params) {
  AssertIR(Parse("select a from b where c = @what", {{"what", "1"}}), "select a from b where c = 1");
  AssertIR(Parse("select a from b where @x = c", {{"x", "2"}}), "select a from b where c = 2");
  AssertIR(Parse("select a from b where c hastag @y", {{"y", "hello"}}), "select a from b where c hastag \"hello\"");
  AssertIR(Parse("select a from b where c hastag @y and @zzz = d", {{"y", "hello"}, {"zzz", "3"}}),
           "select a from b where (and c hastag \"hello\", d = 3)");
  ASSERT_EQ(Parse("select a from b where c hastag @y", {{"z", "hello"}}).Msg(), "parameter with name `y` not found");
}

TEST(SQLParserTest, Vector) {
  AssertSyntaxError(Parse("select a from b where embedding <-> [3,1,2]"));
  AssertSyntaxError(Parse("select a from b where embedding <-> [3,1,2] <"));
  AssertSyntaxError(Parse("select a from b where embedding [3,1,2] < 3"));
  AssertSyntaxError(Parse("select a from b where embedding <> [3,1,2] < 4"));
  AssertSyntaxError(Parse("select a from b where embedding <- [3,1,2] < 3"));
  AssertSyntaxError(Parse("select a from b order by embedding <-> [1,2,3] < 3"));
  AssertSyntaxError(Parse("select a from b where embedding <-> [1,2,3] limit 5"));
  AssertSyntaxError(Parse("select a from b where [3,1,2] <-> embedding < 5"));
  AssertSyntaxError(Parse("select a from b where embedding <-> [] < 5"));
  AssertSyntaxError(Parse("select a from b order by embedding <-> @vec limit 5", {{"vec", "[3.6,7.8]"}}));
  AssertSyntaxError(Parse("select a from b where embedding <#> [3,1,2] < 5"));
  AssertSyntaxError(Parse("select a from b order by embedding <-> [3,1,2] desc limit 5"));

  AssertIR(Parse("select a from b where embedding <-> [3,1,2] < 5"),
           "select a from b where embedding <-> [3.000000, 1.000000, 2.000000] < 5");
  AssertIR(Parse("select a from b where embedding <-> [0.5,0.5] < 10 and c > 100"),
           "select a from b where (and embedding <-> [0.500000, 0.500000] < 10, c > 100)");
  AssertIR(Parse("select a from b order by embedding <-> [3.6] limit 5"),
           "select a from b where true sortby embedding <-> [3.600000] limit 0, 5");
}
