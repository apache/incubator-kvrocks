/*
 * Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>.
 * Copyright (c) 2015-2016, Salvatore Sanfilippo <antirez@gmail.com>.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

/* This is a C to C++ conversion from the redis project.
 * This file started out as:
 * https://github.com/antirez/redis/blob/b2cd9fc/src/geohash_helper.c
 * + https://github.com/antirez/redis/blob/a036c64/src/geohash.c
 */

#include "geohash.h"

#include <math.h>

constexpr double D_R = M_PI / 180.0;

// @brief The usual PI/180 constant
// const double DEG_TO_RAD = 0.017453292519943295769236907684886;
// @brief Earth's quadratic mean radius for WGS-84
const double EARTH_RADIUS_IN_METERS = 6372797.560856;

const double MERCATOR_MAX = 20037726.37;
// const double MERCATOR_MIN = -20037726.37;

static inline double DegRad(double ang) { return ang * D_R; }
static inline double RadDeg(double ang) { return ang / D_R; }

/**
 * Hashing works like this:
 * Divide the world into 4 buckets.  Label each one as such:
 *  -----------------
 *  |       |       |
 *  |       |       |
 *  | 0,1   | 1,1   |
 *  -----------------
 *  |       |       |
 *  |       |       |
 *  | 0,0   | 1,0   |
 *  -----------------
 */

/* Interleave lower bits of x and y, so the bits of x
 * are in the even positions and bits from y in the odd;
 * x and y must initially be less than 2**32 (65536).
 * From:  https://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN
 */
static inline uint64_t Interleave64(uint32_t xlo, uint32_t ylo) {
  static const uint64_t B[] = {0x5555555555555555ULL, 0x3333333333333333ULL, 0x0F0F0F0F0F0F0F0FULL,
                               0x00FF00FF00FF00FFULL, 0x0000FFFF0000FFFFULL};
  static const unsigned int S[] = {1, 2, 4, 8, 16};

  uint64_t x = xlo;
  uint64_t y = ylo;

  x = (x | (x << S[4])) & B[4];
  y = (y | (y << S[4])) & B[4];

  x = (x | (x << S[3])) & B[3];
  y = (y | (y << S[3])) & B[3];

  x = (x | (x << S[2])) & B[2];
  y = (y | (y << S[2])) & B[2];

  x = (x | (x << S[1])) & B[1];
  y = (y | (y << S[1])) & B[1];

  x = (x | (x << S[0])) & B[0];
  y = (y | (y << S[0])) & B[0];

  return x | (y << 1);
}

/* reverse the interleave process
 * derived from http://stackoverflow.com/questions/4909263
 */
static inline uint64_t Deinterleave64(uint64_t interleaved) {
  static const uint64_t B[] = {0x5555555555555555ULL, 0x3333333333333333ULL, 0x0F0F0F0F0F0F0F0FULL,
                               0x00FF00FF00FF00FFULL, 0x0000FFFF0000FFFFULL, 0x00000000FFFFFFFFULL};
  static const unsigned int S[] = {0, 1, 2, 4, 8, 16};

  uint64_t x = interleaved;
  uint64_t y = interleaved >> 1;

  x = (x | (x >> S[0])) & B[0];
  y = (y | (y >> S[0])) & B[0];

  x = (x | (x >> S[1])) & B[1];
  y = (y | (y >> S[1])) & B[1];

  x = (x | (x >> S[2])) & B[2];
  y = (y | (y >> S[2])) & B[2];

  x = (x | (x >> S[3])) & B[3];
  y = (y | (y >> S[3])) & B[3];

  x = (x | (x >> S[4])) & B[4];
  y = (y | (y >> S[4])) & B[4];

  x = (x | (x >> S[5])) & B[5];
  y = (y | (y >> S[5])) & B[5];

  return x | (y << 32);
}

void GeohashGetCoordRange(GeoHashRange *long_range, GeoHashRange *lat_range) {
  /* These are constraints from EPSG:900913 / EPSG:3785 / OSGEO:41001 */
  /* We can't geocode at the north/south pole. */
  long_range->max = GEO_LONG_MAX;
  long_range->min = GEO_LONG_MIN;
  lat_range->max = GEO_LAT_MAX;
  lat_range->min = GEO_LAT_MIN;
}

int GeohashEncode(const GeoHashRange *long_range, const GeoHashRange *lat_range, double longitude, double latitude,
                  uint8_t step, GeoHashBits *hash) {
  /* Check basic arguments sanity. */
  if (!hash || step > 32 || step == 0 || RANGEPISZERO(lat_range) || RANGEPISZERO(long_range)) return 0;

  /* Return an error when trying to index outside the supported
   * constraints. */
  if (longitude > GEO_LONG_MAX || longitude < GEO_LONG_MIN || latitude > GEO_LAT_MAX || latitude < GEO_LAT_MIN)
    return 0;

  hash->bits = 0;
  hash->step = step;

  if (latitude < lat_range->min || latitude > lat_range->max || longitude < long_range->min ||
      longitude > long_range->max) {
    return 0;
  }

  double lat_offset = (latitude - lat_range->min) / (lat_range->max - lat_range->min);
  double long_offset = (longitude - long_range->min) / (long_range->max - long_range->min);

  /* convert to fixed point based on the step size */
  lat_offset *= static_cast<double>(1ULL << step);
  long_offset *= static_cast<double>(1ULL << step);
  hash->bits = Interleave64(static_cast<uint32_t>(lat_offset), static_cast<uint32_t>(long_offset));
  return 1;
}

int GeohashEncodeType(double longitude, double latitude, uint8_t step, GeoHashBits *hash) {
  GeoHashRange r[2] = {{0}};
  GeohashGetCoordRange(&r[0], &r[1]);
  return GeohashEncode(&r[0], &r[1], longitude, latitude, step, hash);
}

int GeohashEncodeWGS84(double longitude, double latitude, uint8_t step, GeoHashBits *hash) {
  return GeohashEncodeType(longitude, latitude, step, hash);
}

int GeohashDecode(const GeoHashRange &long_range, const GeoHashRange &lat_range, const GeoHashBits &hash,
                  GeoHashArea *area) {
  if (HASHISZERO(hash) || !area || RANGEISZERO(lat_range) || RANGEISZERO(long_range)) {
    return 0;
  }

  area->hash = hash;
  uint8_t step = hash.step;
  uint64_t hash_sep = Deinterleave64(hash.bits); /* hash = [LAT][LONG] */

  double lat_scale = lat_range.max - lat_range.min;
  double long_scale = long_range.max - long_range.min;

  uint32_t ilato = hash_sep;       /* get lat part of deinterleaved hash */
  uint32_t ilono = hash_sep >> 32; /* shift over to get long part of hash */

  /* divide by 2**step.
   * Then, for 0-1 coordinate, multiply times scale and add
     to the min to get the absolute coordinate. */
  area->latitude.min = lat_range.min + (ilato * 1.0 / static_cast<double>(1ull << step)) * lat_scale;
  area->latitude.max = lat_range.min + ((ilato + 1) * 1.0 / static_cast<double>(1ull << step)) * lat_scale;
  area->longitude.min = long_range.min + (ilono * 1.0 / static_cast<double>(1ull << step)) * long_scale;
  area->longitude.max = long_range.min + ((ilono + 1) * 1.0 / static_cast<double>(1ull << step)) * long_scale;

  return 1;
}

int GeohashDecodeType(const GeoHashBits &hash, GeoHashArea *area) {
  GeoHashRange r[2] = {{0}};
  GeohashGetCoordRange(&r[0], &r[1]);
  return GeohashDecode(r[0], r[1], hash, area);
}

int GeohashDecodeWGS84(const GeoHashBits &hash, GeoHashArea *area) { return GeohashDecodeType(hash, area); }

int GeohashDecodeAreaToLongLat(const GeoHashArea *area, double *xy) {
  if (!xy) return 0;
  xy[0] = (area->longitude.min + area->longitude.max) / 2;
  if (xy[0] > GEO_LONG_MAX) xy[0] = GEO_LONG_MAX;
  if (xy[0] < GEO_LONG_MIN) xy[0] = GEO_LONG_MIN;
  xy[1] = (area->latitude.min + area->latitude.max) / 2;
  if (xy[1] > GEO_LAT_MAX) xy[1] = GEO_LAT_MAX;
  if (xy[1] < GEO_LAT_MIN) xy[1] = GEO_LAT_MIN;
  return 1;
}

int GeohashDecodeToLongLatType(const GeoHashBits &hash, double *xy) {
  GeoHashArea area = {{0}};
  if (!xy || !GeohashDecodeType(hash, &area)) return 0;
  return GeohashDecodeAreaToLongLat(&area, xy);
}

int GeohashDecodeToLongLatWGS84(const GeoHashBits &hash, double *xy) { return GeohashDecodeToLongLatType(hash, xy); }

static void GeohashMoveX(GeoHashBits *hash, int8_t d) {
  if (d == 0) return;

  uint64_t x = hash->bits & 0xaaaaaaaaaaaaaaaaULL;
  uint64_t y = hash->bits & 0x5555555555555555ULL;

  uint64_t zz = 0x5555555555555555ULL >> (64 - hash->step * 2);  // NOLINT

  if (d > 0) {
    x = x + (zz + 1);
  } else {
    x = x | zz;
    x = x - (zz + 1);
  }

  x &= (0xaaaaaaaaaaaaaaaaULL >> (64 - hash->step * 2));  // NOLINT
  hash->bits = (x | y);
}

static void GeohashMoveY(GeoHashBits *hash, int8_t d) {
  if (d == 0) return;

  uint64_t x = hash->bits & 0xaaaaaaaaaaaaaaaaULL;
  uint64_t y = hash->bits & 0x5555555555555555ULL;

  uint64_t zz = 0xaaaaaaaaaaaaaaaaULL >> (64 - hash->step * 2);
  if (d > 0) {
    y = y + (zz + 1);
  } else {
    y = y | zz;
    y = y - (zz + 1);
  }
  y &= (0x5555555555555555ULL >> (64 - hash->step * 2));
  hash->bits = (x | y);
}

void GeohashNeighbors(const GeoHashBits *hash, GeoHashNeighbors *neighbors) {
  neighbors->east = *hash;
  neighbors->west = *hash;
  neighbors->north = *hash;
  neighbors->south = *hash;
  neighbors->south_east = *hash;
  neighbors->south_west = *hash;
  neighbors->north_east = *hash;
  neighbors->north_west = *hash;

  GeohashMoveX(&neighbors->east, 1);
  GeohashMoveY(&neighbors->east, 0);

  GeohashMoveX(&neighbors->west, -1);
  GeohashMoveY(&neighbors->west, 0);

  GeohashMoveX(&neighbors->south, 0);
  GeohashMoveY(&neighbors->south, -1);

  GeohashMoveX(&neighbors->north, 0);
  GeohashMoveY(&neighbors->north, 1);

  GeohashMoveX(&neighbors->north_west, -1);
  GeohashMoveY(&neighbors->north_west, 1);

  GeohashMoveX(&neighbors->north_east, 1);
  GeohashMoveY(&neighbors->north_east, 1);

  GeohashMoveX(&neighbors->south_east, 1);
  GeohashMoveY(&neighbors->south_east, -1);

  GeohashMoveX(&neighbors->south_west, -1);
  GeohashMoveY(&neighbors->south_west, -1);
}

/* This function is used in order to estimate the step (bits precision)
 * of the 9 search area boxes during radius queries. */
uint8_t GeoHashHelper::EstimateStepsByRadius(double range_meters, double lat) {
  if (range_meters == 0) return 26;
  int step = 1;
  while (range_meters < MERCATOR_MAX) {
    range_meters *= 2;
    step++;
  }
  step -= 2; /* Make sure range is included in most of the base cases. */

  /* Wider range towards the poles... Note: it is possible to do better
   * than this approximation by computing the distance between meridians
   * at this latitude, but this does the trick for now. */
  if (lat > 66 || lat < -66) {
    step--;
    if (lat > 80 || lat < -80) step--;
  }

  /* Frame to valid range. */
  if (step < 1) step = 1;
  if (step > 26) step = 26;
  return step;
}

/* Return the bounding box of the search area centered at latitude,longitude
 * having a radius of radius_meter. bounds[0] - bounds[2] is the minimum
 * and maximum longitude, while bounds[1] - bounds[3] is the minimum and
 * maximum latitude. */
int GeoHashHelper::BoundingBox(GeoShape *geo_shape) {
  if (!geo_shape) return 0;
  double longitude = geo_shape->xy[0];
  double latitude = geo_shape->xy[1];
  double height =
      geo_shape->conversion * (geo_shape->type == kGeoShapeTypeCircular ? geo_shape->radius : geo_shape->height / 2);
  double width =
      geo_shape->conversion * (geo_shape->type == kGeoShapeTypeCircular ? geo_shape->radius : geo_shape->width / 2);

  const double lat_delta = RadDeg(height / EARTH_RADIUS_IN_METERS);
  const double long_delta_top = RadDeg(width / EARTH_RADIUS_IN_METERS / cos(DegRad(latitude + lat_delta)));
  const double long_delta_bottom = RadDeg(width / EARTH_RADIUS_IN_METERS / cos(DegRad(latitude - lat_delta)));

  bool is_in_southern_hemisphere = latitude < 0;
  geo_shape->bounds[0] = is_in_southern_hemisphere ? longitude - long_delta_bottom : longitude - long_delta_top;
  geo_shape->bounds[2] = is_in_southern_hemisphere ? longitude + long_delta_bottom : longitude + long_delta_top;
  geo_shape->bounds[1] = latitude - lat_delta;
  geo_shape->bounds[3] = latitude + lat_delta;
  return 1;
}

GeoHashRadius GeoHashHelper::GetAreasByShapeWGS84(GeoShape &geo_shape) {
  GeoHashRange long_range, lat_range;
  GeoHashRadius radius;
  GeoHashBits hash;
  GeoHashNeighbors neighbors;
  GeoHashArea area;
  double min_lon = NAN, max_lon = NAN, min_lat = NAN, max_lat = NAN;

  BoundingBox(&geo_shape);
  min_lon = geo_shape.bounds[0];
  min_lat = geo_shape.bounds[1];
  max_lon = geo_shape.bounds[2];
  max_lat = geo_shape.bounds[3];

  double longitude = geo_shape.xy[0];
  double latitude = geo_shape.xy[1];

  double radius_meters =
      geo_shape.conversion * (geo_shape.type == kGeoShapeTypeCircular
                                  ? geo_shape.radius
                                  : sqrt(pow((geo_shape.width / 2), 2) + pow((geo_shape.height / 2), 2)));

  int steps = EstimateStepsByRadius(radius_meters, latitude);

  GeohashGetCoordRange(&long_range, &lat_range);
  GeohashEncode(&long_range, &lat_range, longitude, latitude, steps, &hash);
  GeohashNeighbors(&hash, &neighbors);
  GeohashDecode(long_range, lat_range, hash, &area);

  /* Check if the step is enough at the limits of the covered area.
   * Sometimes when the search area is near an edge of the
   * area, the estimated step is not small enough, since one of the
   * north / south / west / east square is too near to the search area
   * to cover everything. */
  int decrease_step = 0;
  {
    GeoHashArea north, south, east, west;

    GeohashDecode(long_range, lat_range, neighbors.north, &north);
    GeohashDecode(long_range, lat_range, neighbors.south, &south);
    GeohashDecode(long_range, lat_range, neighbors.east, &east);
    GeohashDecode(long_range, lat_range, neighbors.west, &west);

    if (GetDistance(longitude, latitude, longitude, north.latitude.max) < radius_meters) decrease_step = 1;
    if (GetDistance(longitude, latitude, longitude, south.latitude.min) < radius_meters) decrease_step = 1;
    if (GetDistance(longitude, latitude, east.longitude.max, latitude) < radius_meters) decrease_step = 1;
    if (GetDistance(longitude, latitude, west.longitude.min, latitude) < radius_meters) decrease_step = 1;
  }

  if (steps > 1 && decrease_step) {
    steps--;
    GeohashEncode(&long_range, &lat_range, longitude, latitude, steps, &hash);
    GeohashNeighbors(&hash, &neighbors);
    GeohashDecode(long_range, lat_range, hash, &area);
  }

  /* Exclude the search areas that are useless. */
  if (steps >= 2) {
    if (area.latitude.min < min_lat) {
      GZERO(neighbors.south);
      GZERO(neighbors.south_west);
      GZERO(neighbors.south_east);
    }
    if (area.latitude.max > max_lat) {
      GZERO(neighbors.north);
      GZERO(neighbors.north_east);
      GZERO(neighbors.north_west);
    }
    if (area.longitude.min < min_lon) {
      GZERO(neighbors.west);
      GZERO(neighbors.south_west);
      GZERO(neighbors.north_west);
    }
    if (area.longitude.max > max_lon) {
      GZERO(neighbors.east);
      GZERO(neighbors.south_east);
      GZERO(neighbors.north_east);
    }
  }
  radius.hash = hash;
  radius.neighbors = neighbors;
  radius.area = area;
  return radius;
}

GeoHashFix52Bits GeoHashHelper::Align52Bits(const GeoHashBits &hash) {
  uint64_t bits = hash.bits;
  bits <<= (52 - hash.step * 2);
  return bits;
}

/* Calculate distance using haversin great circle distance formula. */
double GeoHashHelper::GetDistance(double lon1d, double lat1d, double lon2d, double lat2d) {
  double lat1r = NAN, lon1r = NAN, lat2r = NAN, lon2r = NAN, u = NAN, v = NAN;
  lat1r = DegRad(lat1d);
  lon1r = DegRad(lon1d);
  lat2r = DegRad(lat2d);
  lon2r = DegRad(lon2d);
  u = sin((lat2r - lat1r) / 2);
  v = sin((lon2r - lon1r) / 2);
  return 2.0 * EARTH_RADIUS_IN_METERS * asin(sqrt(u * u + cos(lat1r) * cos(lat2r) * v * v));
}

int GeoHashHelper::GetDistanceIfInRadius(double x1, double y1, double x2, double y2, double radius, double *distance) {
  *distance = GetDistance(x1, y1, x2, y2);
  if (*distance > radius) return 0;
  return 1;
}

int GeoHashHelper::GetDistanceIfInBox(const double *bounds, double x1, double y1, double x2, double y2,
                                      double *distance) {
  if (x2 < bounds[0] || x2 > bounds[2] || y2 < bounds[1] || y2 > bounds[3]) return 0;
  *distance = GetDistance(x1, y1, x2, y2);
  return 1;
}

int GeoHashHelper::GetDistanceIfInRadiusWGS84(double x1, double y1, double x2, double y2, double radius,
                                              double *distance) {
  return GetDistanceIfInRadius(x1, y1, x2, y2, radius, distance);
}

int GeoHashHelper::GetDistanceIfInBoxWGS84(const double *bounds, double x1, double y1, double x2, double y2,
                                           double *distance) {
  return GetDistanceIfInBox(bounds, x1, y1, x2, y2, distance);
}
