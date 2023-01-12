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

#include "geomserde.h"
#include "geos_c_dyn.h"

#include <stdlib.h>

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
typedef enum GeometryTypeId {
  POINT = 1,
  LINESTRING = 2,
  POLYGON = 3,
  MULTIPOINT = 4,
  MULTILINESTRING = 5,
  MULTIPOLYGON = 6,
  GEOMETRYCOLLECTION = 7
} GeometryTypeId;

static inline CoordinateType coordinate_type_of(int has_z, int has_m) {
  /* libgeos currently do not support M dimensions, so we simply ignore
   * has_m */
  if (has_z) {
    return XYZ;
  } else {
    return XY;
  }
}

static inline int get_bytes_per_coordinate(CoordinateType coord_type) {
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

static void *alloc_buffer_for_geom(GeometryTypeId geom_type_id,
                                   CoordinateType coord_type, int srid,
                                   int buf_size, int num_coords) {  
  unsigned char *buf = malloc(buf_size);
  int *buf_int = (int *) buf;
  if (buf == NULL) {
    return buf;
  }
  int has_srid = (srid != 0)? 1: 0;
  unsigned char preamble_byte = (geom_type_id << 4) | (coord_type << 1) | has_srid;
  buf[0] = preamble_byte;
  buf[1] = srid >> 16;
  buf[2] = srid >> 8;
  buf[3] = srid;
  buf_int[1] = num_coords;
  return buf;
}

static inline int copy_coord_seq_to_buffer(GEOSContextHandle_t handle,
                                           const GEOSCoordSequence *coord_seq,
                                           double *buf, int has_z, int has_m) {
  if (dyn_GEOSCoordSeq_copyToBuffer_r != NULL) {
    /* fast path for libgeos >= 3.10.0 */
    return dyn_GEOSCoordSeq_copyToBuffer_r(handle, coord_seq, buf, has_z,
                                           has_m);
  }

  /* slow path for old libgeos */
  unsigned int num_coords = 0;
  if (dyn_GEOSCoordSeq_getSize_r(handle, coord_seq, &num_coords) == 0) {
    return 0;
  }
  for (unsigned int k = 0; k < num_coords; k++) {
    double x, y, z;
    if (has_z) {
      if (dyn_GEOSCoordSeq_getXYZ_r(handle, coord_seq, k, &x, &y, &z) == 0) {
        return 0;
      }
      *buf++ = x;
      *buf++ = y;
      *buf++ = z;      
    } else {
      if (dyn_GEOSCoordSeq_getXY_r(handle, coord_seq, k, &x, &y) == 0) {
        return 0;
      }
      *buf++ = x;
      *buf++ = y;      
    }
  }
  return 1;
}

static int sedona_serialize_point(GEOSGeometry *geom,
                                  GEOSContextHandle_t handle, char **p_buf,
                                  int *p_buf_size) {
  int srid = dyn_GEOSGetSRID_r(handle, geom);  
  const GEOSCoordSequence *coord_seq = dyn_GEOSGeom_getCoordSeq_r(handle, geom); 
  if (coord_seq == NULL) {
    return SEDONA_GEOS_ERROR;
  }
  unsigned int dims = 0;
  if (dyn_GEOSCoordSeq_getDimensions_r(handle, coord_seq, &dims) == 0) {
    return SEDONA_GEOS_ERROR;
  }
  int has_z = (dims >= 3);
  int has_m = 0;
  CoordinateType coord_type = coordinate_type_of(has_z, has_m);
  int bytes_per_coord = get_bytes_per_coordinate(coord_type);
  unsigned int num_coords = 0;
  if (dyn_GEOSCoordSeq_getSize_r(handle, coord_seq, &num_coords) == 0) {
    return SEDONA_GEOS_ERROR;
  }

  void *buf = NULL;
  if (num_coords == 0) {
    buf = alloc_buffer_for_geom(POINT, coord_type, srid, 8, 0);
    if (buf == NULL) {
      return SEDONA_ALLOC_ERROR;
    }
    *p_buf = buf;
    *p_buf_size = 8;
    return SEDONA_SUCCESS;
  }

  int buf_size = 8 + bytes_per_coord;
  buf = alloc_buffer_for_geom(POINT, coord_type, srid, buf_size, 1);
  if (buf == NULL) {
    return SEDONA_ALLOC_ERROR;
  }
  if (copy_coord_seq_to_buffer(handle, coord_seq, buf + 8, has_z, has_m) == 0) {    
    free(buf);
    return SEDONA_GEOS_ERROR;
  }

  *p_buf = buf;
  *p_buf_size = buf_size;
  return SEDONA_SUCCESS;
}

static int sedona_serialize_linestring(GEOSGeometry *geom,
                                       GEOSContextHandle_t handle, char **p_buf,
                                       int *p_buf_size) {
  return SEDONA_SUCCESS;
}

static int sedona_serialize_polygon(GEOSGeometry *geom,
                                    GEOSContextHandle_t handle, char **p_buf,
                                    int *p_buf_size) {
  return SEDONA_SUCCESS;
}

static int sedona_serialize_multipoint(GEOSGeometry *geom,
                                       GEOSContextHandle_t handle, char **p_buf,
                                       int *p_buf_size) {
  return SEDONA_SUCCESS;
}

static int sedona_serialize_multilinestring(GEOSGeometry *geom,
                                            GEOSContextHandle_t handle,
                                            char **p_buf, int *p_buf_size) {
  return SEDONA_SUCCESS;  
}

static int sedona_serialize_multipolygon(GEOSGeometry *geom,
                                         GEOSContextHandle_t handle,
                                         char **p_buf, int *p_buf_size) {
  return SEDONA_SUCCESS;
}

static int sedona_serialize_geometrycollection(GEOSGeometry *geom,
                                               GEOSContextHandle_t handle,
                                               char **p_buf, int *p_buf_size) {  
  return SEDONA_SUCCESS;
}

int sedona_serialize_geom(GEOSGeometry *geom, GEOSContextHandle_t handle,
                          char **p_buf, int *p_buf_size) {
  int geom_type_id = dyn_GEOSGeomTypeId_r(handle, geom);
  int errcode = SEDONA_SUCCESS;
  switch (geom_type_id) {
    case GEOS_POINT:
      errcode = sedona_serialize_point(geom, handle, p_buf, p_buf_size);
      break;
    case GEOS_LINESTRING:
      errcode = sedona_serialize_linestring(geom, handle, p_buf, p_buf_size);
      break;
    case GEOS_LINEARRING:
      errcode = SEDONA_UNSUPPORTED_GEOM_TYPE;      
    case GEOS_POLYGON:
      errcode = sedona_serialize_polygon(geom, handle, p_buf, p_buf_size);
      break;
    case GEOS_MULTIPOINT:
      errcode = sedona_serialize_multipoint(geom, handle, p_buf, p_buf_size);
      break;
    case GEOS_MULTILINESTRING:
      errcode =
          sedona_serialize_multilinestring(geom, handle, p_buf, p_buf_size);
      break;
    case GEOS_MULTIPOLYGON:
      errcode = sedona_serialize_multipolygon(geom, handle, p_buf, p_buf_size);
      break;
    case GEOS_GEOMETRYCOLLECTION:
      errcode = sedona_serialize_geometrycollection(geom, handle, p_buf, p_buf_size);
      break;
    default:      
      errcode = SEDONA_UNKNOWN_GEOM_TYPE;
  }

  return errcode;
}

int sedona_deserialize_geom(const char *buf, int buf_size,
                            GEOSContextHandle_t handle, GEOSGeometry **p_geom) {  
  GEOSGeometry *geom = dyn_GEOSGeom_createPointFromXY_r(handle, 10.0, 20.0);
  *p_geom = geom;
  return SEDONA_SUCCESS;
}

const char *sedona_get_error_message(int err) {
  switch (err) {
    case SEDONA_SUCCESS:
      return "";
    case SEDONA_UNKNOWN_GEOM_TYPE:
      return "Unknown geometry type";
    case SEDONA_UNSUPPORTED_GEOM_TYPE:
      return "Unsupported geometry type";
    case SEDONA_GEOS_ERROR:
      return "GEOS error";
    default:
      return "Unknown failure occurred";
  }
}
