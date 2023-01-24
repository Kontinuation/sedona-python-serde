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

#include <stdlib.h>

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
typedef enum GeometryTypeId {
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

static CoordinateType coordinate_type_of(int has_z, int has_m) {
  /* libgeos currently do not support M dimensions, so we simply ignore
   * has_m */
  if (has_z) {
    return XYZ;
  } else {
    return XY;
  }
}

static unsigned int get_bytes_per_coordinate(CoordinateType coord_type) {
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

static SedonaErrorCode get_coord_seq_info(
    GEOSContextHandle_t handle, const GEOSCoordSequence *coord_seq,
    CoordinateSequenceInfo *coord_seq_info) {
  unsigned int dims = 0;
  if (dyn_GEOSCoordSeq_getDimensions_r(handle, coord_seq, &dims) == 0) {
    return SEDONA_GEOS_ERROR;
  }
  int has_z = (dims >= 3);
  int has_m = 0; /* libgeos does not support M dimension for now */
  CoordinateType coord_type = coordinate_type_of(has_z, has_m);
  unsigned int bytes_per_coord = get_bytes_per_coordinate(coord_type);
  unsigned int num_coords = 0;
  if (dyn_GEOSCoordSeq_getSize_r(handle, coord_seq, &num_coords) == 0) {
    return SEDONA_GEOS_ERROR;
  }

  coord_seq_info->dims = dims;
  coord_seq_info->has_z = has_z;
  coord_seq_info->has_m = has_m;
  coord_seq_info->coord_type = coord_type;
  coord_seq_info->bytes_per_coord = bytes_per_coord;
  coord_seq_info->num_coords = num_coords;
  coord_seq_info->total_bytes = bytes_per_coord * num_coords;
  return SEDONA_SUCCESS;
}

static SedonaErrorCode get_coord_seq_info_from_geom(
    GEOSContextHandle_t handle, const GEOSGeometry *geom,
    CoordinateSequenceInfo *coord_seq_info) {
  int dims = dyn_GEOSGeom_getCoordinateDimension_r(handle, geom);
  if (dims == 0) {
    return SEDONA_GEOS_ERROR;
  }
  int has_z = (dims >= 3);
  int has_m = 0; /* libgeos does not support M dimension for now */
  CoordinateType coord_type = coordinate_type_of(has_z, has_m);
  unsigned int bytes_per_coord = get_bytes_per_coordinate(coord_type);
  int num_coords = dyn_GEOSGetNumCoordinates_r(handle, geom);
  if (num_coords == -1) {
    return SEDONA_GEOS_ERROR;
  }

  coord_seq_info->dims = dims;
  coord_seq_info->has_z = has_z;
  coord_seq_info->has_m = has_m;
  coord_seq_info->coord_type = coord_type;
  coord_seq_info->bytes_per_coord = bytes_per_coord;
  coord_seq_info->num_coords = num_coords;
  coord_seq_info->total_bytes = bytes_per_coord * num_coords;
  return SEDONA_SUCCESS;
}

static void *alloc_buffer_for_geom(GeometryTypeId geom_type_id,
                                   CoordinateType coord_type, int srid,
                                   int buf_size, int num_coords) {
  unsigned char *buf = malloc(buf_size);
  int *buf_int = (int *)buf;
  if (buf == NULL) {
    return buf;
  }
  int has_srid = (srid != 0) ? 1 : 0;
  unsigned char preamble_byte =
      (geom_type_id << 4) | (coord_type << 1) | has_srid;
  buf[0] = preamble_byte;
  buf[1] = srid >> 16;
  buf[2] = srid >> 8;
  buf[3] = srid;
  buf_int[1] = num_coords;
  return buf;
}

static SedonaErrorCode copy_coord_seq_to_buffer(
    GEOSContextHandle_t handle, const GEOSCoordSequence *coord_seq, double *buf,
    int has_z, int has_m) {
  if (dyn_GEOSCoordSeq_copyToBuffer_r != NULL) {
    /* fast path for libgeos >= 3.10.0 */
    if (dyn_GEOSCoordSeq_copyToBuffer_r(handle, coord_seq, buf, has_z, has_m) ==
        0) {
      return SEDONA_GEOS_ERROR;
    }
    return SEDONA_SUCCESS;
  }

  /* slow path for old libgeos */
  unsigned int num_coords = 0;
  if (dyn_GEOSCoordSeq_getSize_r(handle, coord_seq, &num_coords) == 0) {
    return 0;
  }
  for (unsigned int k = 0; k < num_coords; k++) {
    /* libgeos does not support M dimension for now, so we ignore has_m. */
    if (has_z) {
      double x, y, z;
      if (dyn_GEOSCoordSeq_getXYZ_r(handle, coord_seq, k, &x, &y, &z) == 0) {
        return SEDONA_GEOS_ERROR;
      }
      *buf++ = x;
      *buf++ = y;
      *buf++ = z;
    } else {
      double x, y;
      if (dyn_GEOSCoordSeq_getXY_r(handle, coord_seq, k, &x, &y) == 0) {
        return SEDONA_GEOS_ERROR;
      }
      *buf++ = x;
      *buf++ = y;
    }
  }
  return SEDONA_SUCCESS;
}

typedef struct GeomBuffer {
  void *buf;
  int buf_size;
  double *buf_coord;
  double *buf_coord_end;
  int *buf_int;
  int *buf_int_end;
} GeomBuffer;

static void geom_buf_init(GeomBuffer *geom_buf, void *buf,
                          const CoordinateSequenceInfo *cs_info, int num_coords,
                          int num_ints) {
  geom_buf->buf = buf;
  geom_buf->buf_size = 8 + num_coords * cs_info->bytes_per_coord + 4 * num_ints;
  geom_buf->buf_coord = buf + 8;
  geom_buf->buf_coord_end = geom_buf->buf_coord + num_coords * cs_info->dims;
  geom_buf->buf_int = (int *)geom_buf->buf_coord_end;
  geom_buf->buf_int_end = geom_buf->buf_int + num_ints;
}

static SedonaErrorCode geom_buf_alloc(GeomBuffer *geom_buf,
                                      GeometryTypeId geom_type_id, int srid,
                                      const CoordinateSequenceInfo *cs_info,
                                      int num_ints) {
  int num_coords = cs_info->num_coords;
  int bytes_per_coord = cs_info->bytes_per_coord;
  int buf_size = 8 + num_coords * bytes_per_coord + 4 * num_ints;
  void *buf = alloc_buffer_for_geom(geom_type_id, cs_info->coord_type, srid,
                                    buf_size, num_coords);
  if (buf == NULL) {
    return SEDONA_ALLOC_ERROR;
  }
  geom_buf_init(geom_buf, buf, cs_info, num_coords, num_ints);
  return SEDONA_SUCCESS;
}

static SedonaErrorCode parse_geom_buf(const char *buf, int buf_size,
                                      GeomBuffer *geom_buf,
                                      CoordinateSequenceInfo *cs_info,
                                      GeometryTypeId *p_geom_type_id,
                                      int *p_srid) {
  if (buf_size < 8) {
    return SEDONA_INVALID_ARG_ERROR;
  }
  unsigned int preamble = (unsigned int)buf[0];
  int srid = 0;
  int geom_type_id = preamble >> 4;
  int coord_type = (preamble & 0x0F) >> 1;
  if ((preamble & 0x01) != 0) {
    srid = (((unsigned int)buf[1]) << 16) | (((unsigned int)buf[2]) << 8) |
           ((unsigned int)buf[3]);
  }
  int num_coords = ((int *)buf)[1];
  if (geom_type_id < 0 || geom_type_id > GEOMETRYCOLLECTION) {
    return SEDONA_UNKNOWN_GEOM_TYPE;
  }
  if (coord_type < 0 || coord_type > XYZM) {
    return SEDONA_UNKNOWN_COORD_TYPE;
  }

  int bytes_per_coord = get_bytes_per_coordinate(coord_type);
  int dims = (coord_type == XYZ ? 3 : 2);
  int has_z = (coord_type == XYZ ? 1 : 0);
  int has_m = 0;
  cs_info->bytes_per_coord = bytes_per_coord;
  cs_info->coord_type = coord_type;
  cs_info->num_coords = num_coords;
  cs_info->dims = dims;
  cs_info->has_z = has_z;
  cs_info->has_z = has_m;

  geom_buf->buf = (void *)buf;
  geom_buf->buf_coord = (double *)(buf + 8);
  geom_buf->buf_coord_end = geom_buf->buf_coord + num_coords * dims;
  geom_buf->buf_int = (int *)geom_buf->buf_coord_end;
  geom_buf->buf_int_end = (int *)(buf + buf_size);
  geom_buf->buf_size = buf_size;

  *p_geom_type_id = geom_type_id;
  *p_srid = srid;
  return SEDONA_SUCCESS;
}

static SedonaErrorCode geom_buf_write_coords(
    GeomBuffer *geom_buf, GEOSContextHandle_t handle,
    const GEOSCoordSequence *coord_seq, const CoordinateSequenceInfo *cs_info) {
  int num_coords = cs_info->num_coords;
  if (num_coords == 0) {
    return SEDONA_SUCCESS;
  }
  int num_doubles = num_coords * cs_info->dims;
  if (geom_buf->buf_coord + num_doubles > geom_buf->buf_coord_end) {
    return SEDONA_INTERNAL_ERROR;
  }
  SedonaErrorCode err = copy_coord_seq_to_buffer(
      handle, coord_seq, geom_buf->buf_coord, cs_info->has_z, cs_info->has_m);
  if (err != SEDONA_SUCCESS) {
    return err;
  }
  geom_buf->buf_coord += num_doubles;
  return SEDONA_SUCCESS;
}

static SedonaErrorCode geom_buf_write_polygon(GeomBuffer *geom_buf,
                                              GEOSContextHandle_t handle,
                                              const GEOSGeometry *geom,
                                              CoordinateSequenceInfo *cs_info) {
  int total_num_coords = cs_info->num_coords;
  if (total_num_coords == 0) {
    /* Write number of rings for empty polygon */
    if (geom_buf->buf_int >= geom_buf->buf_int_end) {
      return SEDONA_INTERNAL_ERROR;
    }
    *geom_buf->buf_int++ = 0;
    return SEDONA_SUCCESS;
  }

  const GEOSGeometry *exterior_ring = dyn_GEOSGetExteriorRing_r(handle, geom);
  if (exterior_ring == NULL) {
    return SEDONA_GEOS_ERROR;
  }
  const GEOSCoordSequence *exterior_cs =
      dyn_GEOSGeom_getCoordSeq_r(handle, exterior_ring);
  if (exterior_cs == NULL) {
    return SEDONA_GEOS_ERROR;
  }
  unsigned int exterior_ring_num_coords = 0;
  if (dyn_GEOSCoordSeq_getSize_r(handle, exterior_cs,
                                 &exterior_ring_num_coords) == 0) {
    return SEDONA_GEOS_ERROR;
  }

  int num_interior_rings = dyn_GEOSGetNumInteriorRings_r(handle, geom);
  if (num_interior_rings == -1) {
    return SEDONA_GEOS_ERROR;
  }

  int num_rings = num_interior_rings + 1;
  if (geom_buf->buf_int_end - geom_buf->buf_int < num_rings + 1) {
    return SEDONA_INTERNAL_ERROR;
  }
  *geom_buf->buf_int++ = num_rings;
  *geom_buf->buf_int++ = exterior_ring_num_coords;

  /* Write exterior ring */
  cs_info->num_coords = exterior_ring_num_coords;
  SedonaErrorCode err =
      geom_buf_write_coords(geom_buf, handle, exterior_cs, cs_info);
  if (err != SEDONA_SUCCESS) {
    return err;
  }

  /* Write interior rings */
  for (int k = 0; k < num_interior_rings; k++) {
    const GEOSGeometry *interior_ring =
        dyn_GEOSGetInteriorRingN_r(handle, geom, k);
    if (interior_ring == NULL) {
      return SEDONA_GEOS_ERROR;
    }
    const GEOSCoordSequence *interior_cs =
        dyn_GEOSGeom_getCoordSeq_r(handle, interior_ring);
    if (interior_cs == NULL) {
      return SEDONA_GEOS_ERROR;
    }
    unsigned int num_coords = 0;
    if (dyn_GEOSCoordSeq_getSize_r(handle, interior_cs, &num_coords) == 0) {
      return SEDONA_GEOS_ERROR;
    }

    cs_info->num_coords = num_coords;
    err = geom_buf_write_coords(geom_buf, handle, interior_cs, cs_info);
    if (err != SEDONA_SUCCESS) {
      return err;
    }
    *geom_buf->buf_int++ = num_coords;
  }

  return SEDONA_SUCCESS;
}

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

static SedonaErrorCode sedona_serialize_point(GEOSContextHandle_t handle,
                                              GEOSGeometry *geom, char **p_buf,
                                              int *p_buf_size) {
  int srid = dyn_GEOSGetSRID_r(handle, geom);
  const GEOSCoordSequence *coord_seq = dyn_GEOSGeom_getCoordSeq_r(handle, geom);
  if (coord_seq == NULL) {
    return SEDONA_GEOS_ERROR;
  }
  CoordinateSequenceInfo cs_info;
  SedonaErrorCode err = get_coord_seq_info(handle, coord_seq, &cs_info);
  if (err != SEDONA_SUCCESS) {
    return err;
  }

  if (cs_info.total_bytes == 0) {
    RETURN_BUFFER_FOR_EMPTY_GEOM(POINT, cs_info.coord_type, srid);
  }

  int buf_size = 8 + cs_info.bytes_per_coord;
  void *buf = alloc_buffer_for_geom(POINT, cs_info.coord_type, srid, buf_size,
                                    cs_info.num_coords);
  if (buf == NULL) {
    return SEDONA_ALLOC_ERROR;
  }

  if (cs_info.num_coords > 0) {
    double *buf_double = buf + 8;
    if (cs_info.has_z) {
      double x, y, z;
      if (dyn_GEOSCoordSeq_getXYZ_r(handle, coord_seq, 0, &x, &y, &z) == 0) {
        goto geos_error;
      }
      buf_double[0] = x;
      buf_double[1] = y;
      buf_double[2] = z;
    } else {
      double x, y;
      if (dyn_GEOSCoordSeq_getXY_r(handle, coord_seq, 0, &x, &y) == 0) {
        goto geos_error;
      }
      buf_double[0] = x;
      buf_double[1] = y;
    }
  }

  *p_buf = buf;
  *p_buf_size = buf_size;
  return SEDONA_SUCCESS;

geos_error:
  free(buf);
  return SEDONA_GEOS_ERROR;
}

static SedonaErrorCode sedona_deserialize_point(GEOSContextHandle_t handle,
                                                int srid, GeomBuffer *geom_buf,
                                                CoordinateSequenceInfo *cs_info,
                                                GEOSGeometry **p_geom) {
  /* TODO: implement this */
  GEOSGeometry *geom = dyn_GEOSGeom_createPointFromXY_r(handle, 10.0, 20.0);
  *p_geom = geom;
  return SEDONA_SUCCESS;
}

static SedonaErrorCode sedona_serialize_linestring(GEOSContextHandle_t handle,
                                                   GEOSGeometry *geom,
                                                   char **p_buf,
                                                   int *p_buf_size) {
  int srid = dyn_GEOSGetSRID_r(handle, geom);
  const GEOSCoordSequence *coord_seq = dyn_GEOSGeom_getCoordSeq_r(handle, geom);
  if (coord_seq == NULL) {
    return SEDONA_GEOS_ERROR;
  }
  CoordinateSequenceInfo cs_info;
  SedonaErrorCode err = get_coord_seq_info(handle, coord_seq, &cs_info);
  if (err != SEDONA_SUCCESS) {
    return err;
  }

  GeomBuffer geom_buf;
  err = geom_buf_alloc(&geom_buf, LINESTRING, srid, &cs_info, 0);
  if (err != SEDONA_SUCCESS) {
    return err;
  }

  err = geom_buf_write_coords(&geom_buf, handle, coord_seq, &cs_info);
  if (err != SEDONA_SUCCESS) {
    free(geom_buf.buf);
    return err;
  }

  *p_buf = geom_buf.buf;
  *p_buf_size = geom_buf.buf_size;
  return SEDONA_SUCCESS;
}

static SedonaErrorCode sedona_deserialize_linestring(
    GEOSContextHandle_t handle, int srid, GeomBuffer *geom_buf,
    CoordinateSequenceInfo *cs_info, GEOSGeometry **p_geom) {
  return SEDONA_UNKNOWN_GEOM_TYPE;
}

static SedonaErrorCode sedona_serialize_polygon(GEOSContextHandle_t handle,
                                                GEOSGeometry *geom,
                                                char **p_buf, int *p_buf_size) {
  int srid = dyn_GEOSGetSRID_r(handle, geom);
  CoordinateSequenceInfo cs_info;
  SedonaErrorCode err = get_coord_seq_info_from_geom(handle, geom, &cs_info);
  if (err != SEDONA_SUCCESS) {
    return err;
  }
  if (cs_info.num_coords == 0) {
    RETURN_BUFFER_FOR_EMPTY_GEOM(POLYGON, cs_info.coord_type, srid);
  }

  int num_interior_rings = dyn_GEOSGetNumInteriorRings_r(handle, geom);
  if (num_interior_rings == -1) {
    return SEDONA_GEOS_ERROR;
  }

  GeomBuffer geom_buf;
  int num_rings = num_interior_rings + 1;
  err = geom_buf_alloc(&geom_buf, POLYGON, srid, &cs_info, num_rings + 1);
  if (err != SEDONA_SUCCESS) {
    return err;
  }

  err = geom_buf_write_polygon(&geom_buf, handle, geom, &cs_info);
  if (err != SEDONA_SUCCESS) {
    free(geom_buf.buf);
    return err;
  }

  *p_buf = geom_buf.buf;
  *p_buf_size = geom_buf.buf_size;
  return SEDONA_SUCCESS;
}

static SedonaErrorCode sedona_deserialize_polygon(
    GEOSContextHandle_t handle, int srid, GeomBuffer *geom_buf,
    CoordinateSequenceInfo *cs_info, GEOSGeometry **p_geom) {
  return SEDONA_UNKNOWN_GEOM_TYPE;
}

static SedonaErrorCode sedona_serialize_multipoint(GEOSContextHandle_t handle,
                                                   GEOSGeometry *geom,
                                                   char **p_buf,
                                                   int *p_buf_size) {
  return SEDONA_UNKNOWN_GEOM_TYPE;
}

static SedonaErrorCode sedona_deserialize_multipoint(
    GEOSContextHandle_t handle, int srid, GeomBuffer *geom_buf,
    CoordinateSequenceInfo *cs_info, GEOSGeometry **p_geom) {
  return SEDONA_UNKNOWN_GEOM_TYPE;
}

static SedonaErrorCode sedona_serialize_multilinestring(
    GEOSContextHandle_t handle, GEOSGeometry *geom, char **p_buf,
    int *p_buf_size) {
  return SEDONA_SUCCESS;
}

static SedonaErrorCode sedona_deserialize_multilinestring(
    GEOSContextHandle_t handle, int srid, GeomBuffer *geom_buf,
    CoordinateSequenceInfo *cs_info, GEOSGeometry **p_geom) {
  return SEDONA_UNKNOWN_GEOM_TYPE;
}

static SedonaErrorCode sedona_serialize_multipolygon(GEOSContextHandle_t handle,
                                                     GEOSGeometry *geom,
                                                     char **p_buf,
                                                     int *p_buf_size) {
  return SEDONA_SUCCESS;
}

static SedonaErrorCode sedona_deserialize_multipolygon(
    GEOSContextHandle_t handle, int srid, GeomBuffer *geom_buf,
    CoordinateSequenceInfo *cs_info, GEOSGeometry **p_geom) {
  return SEDONA_UNKNOWN_GEOM_TYPE;
}

static SedonaErrorCode sedona_serialize_geometrycollection(
    GEOSContextHandle_t handle, GEOSGeometry *geom, char **p_buf,
    int *p_buf_size) {
  return SEDONA_SUCCESS;
}

static SedonaErrorCode sedona_deserialize_geometrycollection(
    GEOSContextHandle_t handle, int srid, GeomBuffer *geom_buf,
    CoordinateSequenceInfo *cs_info, GEOSGeometry **p_geom) {
  return SEDONA_UNKNOWN_GEOM_TYPE;
}

SedonaErrorCode sedona_serialize_geom(GEOSContextHandle_t handle,
                                      GEOSGeometry *geom, char **p_buf,
                                      int *p_buf_size) {
  int geom_type_id = dyn_GEOSGeomTypeId_r(handle, geom);
  SedonaErrorCode errcode = SEDONA_SUCCESS;
  switch (geom_type_id) {
    case GEOS_POINT:
      errcode = sedona_serialize_point(handle, geom, p_buf, p_buf_size);
      break;
    case GEOS_LINESTRING:
      errcode = sedona_serialize_linestring(handle, geom, p_buf, p_buf_size);
      break;
    case GEOS_LINEARRING:
      errcode = SEDONA_UNSUPPORTED_GEOM_TYPE;
    case GEOS_POLYGON:
      errcode = sedona_serialize_polygon(handle, geom, p_buf, p_buf_size);
      break;
    case GEOS_MULTIPOINT:
      errcode = sedona_serialize_multipoint(handle, geom, p_buf, p_buf_size);
      break;
    case GEOS_MULTILINESTRING:
      errcode =
          sedona_serialize_multilinestring(handle, geom, p_buf, p_buf_size);
      break;
    case GEOS_MULTIPOLYGON:
      errcode = sedona_serialize_multipolygon(handle, geom, p_buf, p_buf_size);
      break;
    case GEOS_GEOMETRYCOLLECTION:
      errcode =
          sedona_serialize_geometrycollection(handle, geom, p_buf, p_buf_size);
      break;
    default:
      errcode = SEDONA_UNKNOWN_GEOM_TYPE;
  }

  return errcode;
}

static SedonaErrorCode deserialize_geom_buf(GEOSContextHandle_t handle,
                                            GeometryTypeId geom_type_id,
                                            int srid, GeomBuffer *geom_buf,
                                            CoordinateSequenceInfo *cs_info,
                                            GEOSGeometry **p_geom) {
  switch (geom_type_id) {
    case POINT:
      return sedona_deserialize_point(handle, srid, geom_buf, cs_info, p_geom);
    case LINESTRING:
      return sedona_deserialize_linestring(handle, srid, geom_buf, cs_info,
                                           p_geom);
    case POLYGON:
      return sedona_deserialize_polygon(handle, srid, geom_buf, cs_info,
                                        p_geom);
    case MULTIPOINT:
      return sedona_deserialize_multipoint(handle, srid, geom_buf, cs_info,
                                           p_geom);
    case MULTILINESTRING:
      return sedona_deserialize_multilinestring(handle, srid, geom_buf, cs_info,
                                                p_geom);
    case MULTIPOLYGON:
      return sedona_deserialize_multipolygon(handle, srid, geom_buf, cs_info,
                                             p_geom);
    case GEOMETRYCOLLECTION:
      return sedona_deserialize_geometrycollection(handle, srid, geom_buf,
                                                   cs_info, p_geom);
    default:
      return SEDONA_UNSUPPORTED_GEOM_TYPE;
  }

  /* TODO: implement this */
  GEOSGeometry *geom = dyn_GEOSGeom_createPointFromXY_r(handle, 10.0, 20.0);
  *p_geom = geom;
  return SEDONA_SUCCESS;
}

SedonaErrorCode sedona_deserialize_geom(GEOSContextHandle_t handle,
                                        const char *buf, int buf_size,
                                        GEOSGeometry **p_geom) {
  GeomBuffer geom_buf;
  CoordinateSequenceInfo cs_info;
  GeometryTypeId geom_type_id;
  int srid;
  SedonaErrorCode err =
      parse_geom_buf(buf, buf_size, &geom_buf, &cs_info, &geom_type_id, &srid);
  if (err != SEDONA_SUCCESS) {
    return err;
  }

  return deserialize_geom_buf(handle, geom_type_id, srid, &geom_buf, &cs_info,
                              p_geom);
}

const char *sedona_get_error_message(int err) {
  switch (err) {
    case SEDONA_SUCCESS:
      return "";
    case SEDONA_UNKNOWN_GEOM_TYPE:
      return "Unknown geometry type";
    case SEDONA_UNKNOWN_COORD_TYPE:
      return "Unknown coordinate type";
    case SEDONA_UNSUPPORTED_GEOM_TYPE:
      return "Unsupported geometry type";
    case SEDONA_INVALID_ARG_ERROR:
      return "Invalid argument";
    case SEDONA_GEOS_ERROR:
      return "GEOS error";
    case SEDONA_ALLOC_ERROR:
      return "Out of memory";
    case SEDONA_INTERNAL_ERROR:
      return "Internal error";
    default:
      return "Unknown failure occurred";
  }
}
