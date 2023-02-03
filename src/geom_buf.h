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

#ifndef GEOM_BUF
#define GEOM_BUF

#include <stdlib.h>

#include "geomserde.h"
#include "geos_c_dyn.h"

/*
 * Constants for identifying geometry dimensions in the serialized buffer of
 * geometry.
 */
typedef enum CoordinateType_enum {
  XY = 1,
  XYZ = 2,
  XYM = 3,
  XYZM = 4
} CoordinateType;

/*
 * Constants for identifying the geometry type in the serialized buffer of
 * geometry.
 */
typedef enum GeometryTypeId_enum {
  POINT = 1,
  LINESTRING = 2,
  POLYGON = 3,
  MULTIPOINT = 4,
  MULTILINESTRING = 5,
  MULTIPOLYGON = 6,
  GEOMETRYCOLLECTION = 7
} GeometryTypeId;

/*
 * Basic info of coordinate sequence retrieved by calling multiple libgeos_c
 * APIs.
 */
typedef struct CoordinateSequenceInfo {
  unsigned int dims;
  int has_z;
  int has_m;
  CoordinateType coord_type;
  unsigned int bytes_per_coord;
  unsigned int num_coords;
  unsigned int total_bytes;
} CoordinateSequenceInfo;

/*
 * Geometry buffer data structure for tracking the reading progress of
 * coordinate part and offset part separately.
 */
typedef struct GeomBuffer {
  void *buf;
  int buf_size;
  double *buf_coord;
  double *buf_coord_end;
  int *buf_int;
  int *buf_int_end;
} GeomBuffer;

inline CoordinateType coordinate_type_of(int has_z, int has_m) {
  /* libgeos currently do not support M dimensions, so we simply ignore
   * has_m */
  if (has_z) {
    return XYZ;
  } else {
    return XY;
  }
}

inline unsigned int get_bytes_per_coordinate(CoordinateType coord_type) {
  switch (coord_type) {
    case XY:
      return 16;
    case XYZ:
    case XYM:
      return 24;
    case XYZM:
    default:
      return 32;
  }
}

extern SedonaErrorCode get_coord_seq_info(
    GEOSContextHandle_t handle, const GEOSCoordSequence *coord_seq,
    CoordinateSequenceInfo *coord_seq_info);

extern SedonaErrorCode get_coord_seq_info_from_geom(
    GEOSContextHandle_t handle, const GEOSGeometry *geom,
    CoordinateSequenceInfo *coord_seq_info);

extern void *alloc_buffer_for_geom(GeometryTypeId geom_type_id,
                                   CoordinateType coord_type, int srid,
                                   int buf_size, int num_coords);

extern SedonaErrorCode geom_buf_alloc(GeomBuffer *geom_buf,
                                      GeometryTypeId geom_type_id, int srid,
                                      const CoordinateSequenceInfo *cs_info,
                                      int num_ints);

extern SedonaErrorCode read_geom_buf_header(const char *buf, int buf_size,
                                            GeomBuffer *geom_buf,
                                            CoordinateSequenceInfo *cs_info,
                                            GeometryTypeId *p_geom_type_id,
                                            int *p_srid);
extern SedonaErrorCode geom_buf_write_coords(
    GeomBuffer *geom_buf, GEOSContextHandle_t handle,
    const GEOSCoordSequence *coord_seq, const CoordinateSequenceInfo *cs_info);

extern SedonaErrorCode geom_buf_read_coords(
    GeomBuffer *geom_buf, GEOSContextHandle_t handle,
    const CoordinateSequenceInfo *cs_info, GEOSCoordSequence **p_coord_seq);

extern SedonaErrorCode geom_buf_write_polygon(GeomBuffer *geom_buf,
                                              GEOSContextHandle_t handle,
                                              const GEOSGeometry *geom,
                                              CoordinateSequenceInfo *cs_info);

extern SedonaErrorCode geom_buf_read_polygon(GeomBuffer *geom_buf,
                                             GEOSContextHandle_t handle,
                                             CoordinateSequenceInfo *cs_info,
                                             GEOSGeometry **p_geom);

#define RETURN_BUFFER_FOR_EMPTY_GEOM(geom_type_id, coord_type, srid)    \
  do {                                                                  \
    void *buf = alloc_buffer_for_geom(POLYGON, coord_type, srid, 8, 0); \
    if (buf == NULL) {                                                  \
      return SEDONA_ALLOC_ERROR;                                        \
    }                                                                   \
    *p_buf = buf;                                                       \
    *p_buf_size = 8;                                                    \
    return SEDONA_SUCCESS;                                              \
  } while (0)

#endif /* GEOM_BUF */
