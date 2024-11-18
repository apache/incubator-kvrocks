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

#include "command_parser.h"
#include "commander.h"
#include "error_constants.h"
#include "server/server.h"
#include "types/geohash.h"
#include "types/redis_geo.h"

namespace redis {

class CommandGeoBase : public Commander {
 public:
  Status ParseDistanceUnit(const std::string &param) {
    if (util::ToLower(param) == "m") {
      distance_unit_ = kDistanceMeter;
    } else if (util::ToLower(param) == "km") {
      distance_unit_ = kDistanceKilometers;
    } else if (util::ToLower(param) == "ft") {
      distance_unit_ = kDistanceFeet;
    } else if (util::ToLower(param) == "mi") {
      distance_unit_ = kDistanceMiles;
    } else {
      return {Status::RedisParseErr, "unsupported unit provided. please use M, KM, FT, MI"};
    }
    return Status::OK();
  }

  static Status ParseLongLat(const std::string &longitude_para, const std::string &latitude_para, double *longitude,
                             double *latitude) {
    auto long_stat = ParseFloat(longitude_para);
    auto lat_stat = ParseFloat(latitude_para);
    if (!long_stat || !lat_stat) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    *longitude = *long_stat;
    *latitude = *lat_stat;

    return ValidateLongLat(longitude, latitude);
  }

  static Status ValidateLongLat(double *longitude, double *latitude) {
    if (*longitude < GEO_LONG_MIN || *longitude > GEO_LONG_MAX || *latitude < GEO_LAT_MIN || *latitude > GEO_LAT_MAX) {
      return {Status::RedisParseErr,
              "invalid longitude,latitude pair " + std::to_string(*longitude) + "," + std::to_string(*latitude)};
    }
    return Status::OK();
  }

  double GetDistanceByUnit(double distance) { return distance / GetUnitConversion(); }

  double GetRadiusMeters(double radius) { return radius * GetUnitConversion(); }

  double GetUnitConversion() {
    double conversion = 0;
    switch (distance_unit_) {
      case kDistanceMeter:
        conversion = 1;
        break;
      case kDistanceKilometers:
        conversion = 1000;
        break;
      case kDistanceFeet:
        conversion = 0.3048;
        break;
      case kDistanceMiles:
        conversion = 1609.34;
        break;
    }
    return conversion;
  }

 protected:
  DistanceUnit distance_unit_ = kDistanceMeter;
};

class CommandGeoAdd : public CommandGeoBase {
 public:
  CommandGeoAdd() : CommandGeoBase() {}
  Status Parse(const std::vector<std::string> &args) override {
    if ((args.size() - 5) % 3 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    for (size_t i = 2; i < args.size(); i += 3) {
      double longitude = 0;
      double latitude = 0;
      auto s = ParseLongLat(args[i], args[i + 1], &longitude, &latitude);
      if (!s.IsOK()) return s;

      geo_points_.emplace_back(GeoPoint{longitude, latitude, args[i + 2]});
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::Geo geo_db(srv->storage, conn->GetNamespace());

    auto s = geo_db.Add(ctx, args_[1], &geo_points_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  std::vector<GeoPoint> geo_points_;
};

class CommandGeoDist : public CommandGeoBase {
 public:
  CommandGeoDist() : CommandGeoBase() {}

  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() == 5) {
      auto s = ParseDistanceUnit(args[4]);
      if (!s.IsOK()) return s;
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    double distance = 0;
    redis::Geo geo_db(srv->storage, conn->GetNamespace());

    auto s = geo_db.Dist(ctx, args_[1], args_[2], args_[3], &distance);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = conn->NilString();
    } else {
      *output = conn->Double(GetDistanceByUnit(distance));
    }
    return Status::OK();
  }
};

class CommandGeoHash : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    for (size_t i = 2; i < args.size(); i++) {
      members_.emplace_back(args[i]);
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<std::string> hashes;
    redis::Geo geo_db(srv->storage, conn->GetNamespace());

    auto s = geo_db.Hash(ctx, args_[1], members_, &hashes);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      hashes.resize(members_.size(), "");
    }

    *output = conn->MultiBulkString(hashes);
    return Status::OK();
  }

 private:
  std::vector<Slice> members_;
};

class CommandGeoPos : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    for (size_t i = 2; i < args.size(); i++) {
      members_.emplace_back(args[i]);
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::map<std::string, GeoPoint> geo_points;
    redis::Geo geo_db(srv->storage, conn->GetNamespace());

    auto s = geo_db.Pos(ctx, args_[1], members_, &geo_points);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> list;

    if (s.IsNotFound()) {
      list.resize(members_.size(), "");
      *output = conn->MultiBulkString(list);
      return Status::OK();
    }

    for (const auto &member : members_) {
      auto iter = geo_points.find(member.ToString());
      if (iter == geo_points.end()) {
        list.emplace_back(conn->NilString());
      } else {
        list.emplace_back(redis::Array({conn->Double(iter->second.longitude), conn->Double(iter->second.latitude)}));
      }
    }
    *output = redis::Array(list);
    return Status::OK();
  }

 private:
  std::vector<Slice> members_;
};

class CommandGeoRadius : public CommandGeoBase {
 public:
  CommandGeoRadius() : CommandGeoBase() {}

  Status Parse(const std::vector<std::string> &args) override {
    auto s = ParseLongLat(args[2], args[3], &longitude_, &latitude_);
    if (!s.IsOK()) return s;

    auto radius = ParseFloat(args[4]);
    if (!radius) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    radius_ = *radius;

    s = ParseDistanceUnit(args[5]);
    if (!s.IsOK()) return s;

    s = ParseRadiusExtraOption();
    if (!s.IsOK()) return s;

    return Commander::Parse(args);
  }

  Status ParseRadiusExtraOption(size_t i = 6) {
    while (i < args_.size()) {
      if (util::ToLower(args_[i]) == "withcoord") {
        with_coord_ = true;
        i++;
      } else if (util::ToLower(args_[i]) == "withdist") {
        with_dist_ = true;
        i++;
      } else if (util::ToLower(args_[i]) == "withhash") {
        with_hash_ = true;
        i++;
      } else if (util::ToLower(args_[i]) == "asc") {
        sort_ = kSortASC;
        i++;
      } else if (util::ToLower(args_[i]) == "desc") {
        sort_ = kSortDESC;
        i++;
      } else if (util::ToLower(args_[i]) == "count" && i + 1 < args_.size()) {
        auto parse_result = ParseInt<int>(args_[i + 1], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        count_ = *parse_result;
        i += 2;
      } else if ((attributes_->InitialFlags() & kCmdWrite) &&
                 (util::ToLower(args_[i]) == "store" || util::ToLower(args_[i]) == "storedist") &&
                 i + 1 < args_.size()) {
        store_key_ = args_[i + 1];
        if (util::ToLower(args_[i]) == "storedist") {
          store_distance_ = true;
        }
        i += 2;
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }
    /* Trap options not compatible with STORE and STOREDIST. */
    if (!store_key_.empty() && (with_dist_ || with_hash_ || with_coord_)) {
      return {Status::RedisParseErr,
              "STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options"};
    }

    /* COUNT without ordering does not make much sense, force ASC
     * ordering if COUNT was specified but no sorting was requested.
     * */
    if (count_ != 0 && sort_ == kSortNone) {
      sort_ = kSortASC;
    }
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<GeoPoint> geo_points;
    redis::Geo geo_db(srv->storage, conn->GetNamespace());

    auto s = geo_db.Radius(ctx, args_[1], longitude_, latitude_, GetRadiusMeters(radius_), count_, sort_, store_key_,
                           store_distance_, GetUnitConversion(), &geo_points);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (store_key_.size() != 0) {
      *output = redis::Integer(geo_points.size());
    } else {
      *output = GenerateOutput(conn, geo_points);
    }
    return Status::OK();
  }

  std::string GenerateOutput(const Connection *conn, const std::vector<GeoPoint> &geo_points) {
    int result_length = static_cast<int>(geo_points.size());
    int returned_items_count = (count_ == 0 || result_length < count_) ? result_length : count_;
    std::vector<std::string> list;
    for (int i = 0; i < returned_items_count; i++) {
      auto geo_point = geo_points[i];
      if (!with_coord_ && !with_hash_ && !with_dist_) {
        list.emplace_back(redis::BulkString(geo_point.member));
      } else {
        std::vector<std::string> one;
        one.emplace_back(redis::BulkString(geo_point.member));
        if (with_dist_) {
          one.emplace_back(conn->Double(GetDistanceByUnit(geo_point.dist)));
        }
        if (with_hash_) {
          one.emplace_back(conn->Double(geo_point.score));
        }
        if (with_coord_) {
          one.emplace_back(redis::Array({conn->Double(geo_point.longitude), conn->Double(geo_point.latitude)}));
        }
        list.emplace_back(redis::Array(one));
      }
    }
    return redis::Array(list);
  }

  static std::vector<CommandKeyRange> Range(const std::vector<std::string> &args) {
    int store_key = 0;

    // Check for the presence of the stored key in the command args.
    for (size_t i = 6; i < args.size(); i++) {
      // For the case when a user specifies both "store" and "storedist" options,
      // the second key will override the first key. The behavior is kept the same
      // as in ParseRadiusExtraOption method.
      if ((util::ToLower(args[i]) == "store" || util::ToLower(args[i]) == "storedist") && i + 1 < args.size()) {
        store_key = (int)i + 1;
        i++;
      }
    }

    if (store_key > 0) {
      return {{1, 1, 1}, {store_key, store_key, 1}};
    }
    return {{1, 1, 1}};
  }

 protected:
  double radius_ = 0;
  bool with_coord_ = false;
  bool with_dist_ = false;
  bool with_hash_ = false;
  int count_ = 0;
  DistanceSort sort_ = kSortNone;
  std::string store_key_;
  bool store_distance_ = false;

 private:
  double longitude_ = 0;
  double latitude_ = 0;
};

class CommandGeoSearch : public CommandGeoBase {
 public:
  CommandGeoSearch() : CommandGeoBase() {}

  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);
    key_ = GET_OR_RET(parser.TakeStr());

    while (parser.Good()) {
      if (parser.EatEqICase("frommember")) {
        auto s = setOriginType(kMember);
        if (!s.IsOK()) return s;

        member_ = GET_OR_RET(parser.TakeStr());
      } else if (parser.EatEqICase("fromlonlat")) {
        auto s = setOriginType(kLongLat);
        if (!s.IsOK()) return s;

        longitude_ = GET_OR_RET(parser.TakeFloat());
        latitude_ = GET_OR_RET(parser.TakeFloat());
        s = ValidateLongLat(&longitude_, &latitude_);
        if (!s.IsOK()) return s;
      } else if (parser.EatEqICase("byradius")) {
        auto s = setShapeType(kGeoShapeTypeCircular);
        if (!s.IsOK()) return s;
        radius_ = GET_OR_RET(parser.TakeFloat());
        std::string distance_raw = GET_OR_RET(parser.TakeStr());
        s = ParseDistanceUnit(distance_raw);
        if (!s.IsOK()) return s;
      } else if (parser.EatEqICase("bybox")) {
        auto s = setShapeType(kGeoShapeTypeRectangular);
        if (!s.IsOK()) return s;
        width_ = GET_OR_RET(parser.TakeFloat());
        height_ = GET_OR_RET(parser.TakeFloat());
        std::string distance_raw = GET_OR_RET(parser.TakeStr());
        s = ParseDistanceUnit(distance_raw);
        if (!s.IsOK()) return s;
      } else if (parser.EatEqICase("asc") && sort_ == kSortNone) {
        sort_ = kSortASC;
      } else if (parser.EatEqICase("desc") && sort_ == kSortNone) {
        sort_ = kSortDESC;
      } else if (parser.EatEqICase("count")) {
        count_ = GET_OR_RET(parser.TakeInt<int>(NumericRange<int>{1, std::numeric_limits<int>::max()}));
      } else if (parser.EatEqICase("withcoord")) {
        with_coord_ = true;
      } else if (parser.EatEqICase("withdist")) {
        with_dist_ = true;
      } else if (parser.EatEqICase("withhash")) {
        with_hash_ = true;
      } else {
        return {Status::RedisParseErr, "Invalid argument given"};
      }
    }

    if (origin_point_type_ == kNone) {
      return {Status::RedisParseErr, "exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH"};
    }

    if (member_ != "" && longitude_ != 0 && latitude_ != 0) {
      return {Status::RedisParseErr, "please use only one of FROMMEMBER or FROMLONLAT"};
    }

    auto s = createGeoShape();
    if (!s.IsOK()) {
      return s;
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<GeoPoint> geo_points;
    redis::Geo geo_db(srv->storage, conn->GetNamespace());

    auto s = geo_db.Search(ctx, args_[1], geo_shape_, origin_point_type_, member_, count_, sort_, false,
                           GetUnitConversion(), &geo_points);

    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = generateOutput(conn, geo_points);

    return Status::OK();
  }

 protected:
  double radius_ = 0;
  double height_ = 0;
  double width_ = 0;
  int count_ = 0;
  double longitude_ = 0;
  double latitude_ = 0;
  std::string member_;
  std::string key_;
  DistanceSort sort_ = kSortNone;
  GeoShapeType shape_type_ = kGeoShapeTypeNone;
  OriginPointType origin_point_type_ = kNone;
  GeoShape geo_shape_;

  Status setShapeType(GeoShapeType shape_type) {
    if (shape_type_ != kGeoShapeTypeNone) {
      return {Status::RedisParseErr, "please use only one of BYBOX or BYRADIUS"};
    }
    shape_type_ = shape_type;
    return Status::OK();
  }

  Status setOriginType(OriginPointType origin_point_type) {
    if (origin_point_type_ != kNone) {
      return {Status::RedisParseErr, "please use only one of FROMMEMBER or FROMLONLAT"};
    }
    origin_point_type_ = origin_point_type;
    return Status::OK();
  }

  Status createGeoShape() {
    if (shape_type_ == kGeoShapeTypeNone) {
      return {Status::RedisParseErr, "please use BYBOX or BYRADIUS"};
    }
    geo_shape_.type = shape_type_;
    geo_shape_.conversion = GetUnitConversion();

    if (shape_type_ == kGeoShapeTypeCircular) {
      geo_shape_.radius = radius_;
    } else {
      geo_shape_.width = width_;
      geo_shape_.height = height_;
    }

    if (origin_point_type_ == kLongLat) {
      geo_shape_.xy[0] = longitude_;
      geo_shape_.xy[1] = latitude_;
    }
    return Status::OK();
  }

  std::string generateOutput(const Connection *conn, const std::vector<GeoPoint> &geo_points) {
    int result_length = static_cast<int>(geo_points.size());
    int returned_items_count = (count_ == 0 || result_length < count_) ? result_length : count_;
    std::vector<std::string> output;
    output.reserve(returned_items_count);
    for (int i = 0; i < returned_items_count; i++) {
      auto geo_point = geo_points[i];
      if (!with_coord_ && !with_hash_ && !with_dist_) {
        output.emplace_back(redis::BulkString(geo_point.member));
      } else {
        std::vector<std::string> one;
        one.emplace_back(redis::BulkString(geo_point.member));
        if (with_dist_) {
          one.emplace_back(conn->Double(GetDistanceByUnit(geo_point.dist)));
        }
        if (with_hash_) {
          one.emplace_back(conn->Double(geo_point.score));
        }
        if (with_coord_) {
          one.emplace_back(redis::Array({conn->Double(geo_point.longitude), conn->Double(geo_point.latitude)}));
        }
        output.emplace_back(redis::Array(one));
      }
    }
    return redis::Array(output);
  }

 private:
  bool with_coord_ = false;
  bool with_dist_ = false;
  bool with_hash_ = false;
};

class CommandGeoSearchStore : public CommandGeoSearch {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);
    store_key_ = GET_OR_RET(parser.TakeStr());
    key_ = GET_OR_RET(parser.TakeStr());

    while (parser.Good()) {
      if (parser.EatEqICase("frommember")) {
        auto s = setOriginType(kMember);
        if (!s.IsOK()) return s;
        member_ = GET_OR_RET(parser.TakeStr());
      } else if (parser.EatEqICase("fromlonlat")) {
        auto s = setOriginType(kLongLat);
        if (!s.IsOK()) return s;

        longitude_ = GET_OR_RET(parser.TakeFloat());
        latitude_ = GET_OR_RET(parser.TakeFloat());
        s = ValidateLongLat(&longitude_, &latitude_);
        if (!s.IsOK()) return s;
      } else if (parser.EatEqICase("byradius")) {
        auto s = setShapeType(kGeoShapeTypeCircular);
        if (!s.IsOK()) return s;
        radius_ = GET_OR_RET(parser.TakeFloat());
        std::string distance_raw = GET_OR_RET(parser.TakeStr());
        s = ParseDistanceUnit(distance_raw);
        if (!s.IsOK()) return s;
      } else if (parser.EatEqICase("bybox")) {
        auto s = setShapeType(kGeoShapeTypeRectangular);
        if (!s.IsOK()) return s;
        width_ = GET_OR_RET(parser.TakeFloat());
        height_ = GET_OR_RET(parser.TakeFloat());
        std::string distance_raw = GET_OR_RET(parser.TakeStr());
        s = ParseDistanceUnit(distance_raw);
        if (!s.IsOK()) return s;
      } else if (parser.EatEqICase("asc") && sort_ == kSortNone) {
        sort_ = kSortASC;
      } else if (parser.EatEqICase("desc") && sort_ == kSortNone) {
        sort_ = kSortDESC;
      } else if (parser.EatEqICase("count")) {
        count_ = GET_OR_RET(parser.TakeInt<int>(NumericRange<int>{1, std::numeric_limits<int>::max()}));
      } else if (parser.EatEqICase("storedist")) {
        store_distance_ = true;
      } else {
        return {Status::RedisParseErr, "Invalid argument given"};
      }
    }

    if (origin_point_type_ == kNone) {
      return {Status::RedisParseErr, "exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCHSTORE"};
    }

    if (member_ != "" && longitude_ != 0 && latitude_ != 0) {
      return {Status::RedisParseErr, "please use only one of FROMMEMBER or FROMLONLAT"};
    }

    auto s = createGeoShape();
    if (!s.IsOK()) {
      return s;
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<GeoPoint> geo_points;
    redis::Geo geo_db(srv->storage, conn->GetNamespace());

    auto s = geo_db.SearchStore(ctx, args_[2], geo_shape_, origin_point_type_, member_, count_, sort_, store_key_,
                                store_distance_, GetUnitConversion(), &geo_points);

    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::Integer(geo_points.size());
    return Status::OK();
  }

  static std::vector<CommandKeyRange> Range([[maybe_unused]] const std::vector<std::string> &args) {
    return {{1, 1, 1}, {2, 2, 1}};
  }

 private:
  bool store_distance_ = false;
  std::string store_key_;
};

class CommandGeoRadiusByMember : public CommandGeoRadius {
 public:
  CommandGeoRadiusByMember() = default;

  Status Parse(const std::vector<std::string> &args) override {
    auto radius = ParseFloat(args[3]);
    if (!radius) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    radius_ = *radius;

    auto s = ParseDistanceUnit(args[4]);
    if (!s.IsOK()) return s;

    s = ParseRadiusExtraOption(5);
    if (!s.IsOK()) return s;

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<GeoPoint> geo_points;
    redis::Geo geo_db(srv->storage, conn->GetNamespace());

    auto s = geo_db.RadiusByMember(ctx, args_[1], args_[2], GetRadiusMeters(radius_), count_, sort_, store_key_,
                                   store_distance_, GetUnitConversion(), &geo_points);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (store_key_.size() != 0) {
      *output = redis::Integer(geo_points.size());
    } else {
      *output = GenerateOutput(conn, geo_points);
    }

    return Status::OK();
  }

  static std::vector<CommandKeyRange> Range(const std::vector<std::string> &args) {
    int store_key = 0;

    // Check for the presence of the stored key in the command args.
    for (size_t i = 5; i < args.size(); i++) {
      // For the case when a user specifies both "store" and "storedist" options,
      // the second key will override the first key. The behavior is kept the same
      // as in ParseRadiusExtraOption method.
      if ((util::ToLower(args[i]) == "store" || util::ToLower(args[i]) == "storedist") && i + 1 < args.size()) {
        store_key = (int)i + 1;
        i++;
      }
    }

    if (store_key > 0) {
      return {{1, 1, 1}, {store_key, store_key, 1}};
    }
    return {{1, 1, 1}};
  }
};

class CommandGeoRadiusReadonly : public CommandGeoRadius {
 public:
  CommandGeoRadiusReadonly() = default;
};

class CommandGeoRadiusByMemberReadonly : public CommandGeoRadiusByMember {
 public:
  CommandGeoRadiusByMemberReadonly() = default;
};

REDIS_REGISTER_COMMANDS(Geo, MakeCmdAttr<CommandGeoAdd>("geoadd", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandGeoDist>("geodist", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandGeoHash>("geohash", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandGeoPos>("geopos", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandGeoRadius>("georadius", -6, "write", CommandGeoRadius::Range),
                        MakeCmdAttr<CommandGeoRadiusByMember>("georadiusbymember", -5, "write",
                                                              CommandGeoRadiusByMember::Range),
                        MakeCmdAttr<CommandGeoRadiusReadonly>("georadius_ro", -6, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandGeoRadiusByMemberReadonly>("georadiusbymember_ro", -5, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandGeoSearch>("geosearch", -7, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandGeoSearchStore>("geosearchstore", -8, "write", CommandGeoSearchStore::Range))

}  // namespace redis
