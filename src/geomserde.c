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

#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "geom_buf.h"
#include "geos_c_dyn.h"

static SedonaErrorCode sedona_serialize_point(GEOSContextHandle_t handle,
                                              const GEOSGeometry *geom,
                                              int srid,
                                              CoordinateSequenceInfo *cs_info,
                                              char **p_buf, int *p_buf_size) {
  if (cs_info->total_bytes == 0) {
    RETURN_BUFFER_FOR_EMPTY_GEOM(POINT, cs_info->coord_type, srid);
  }

  const GEOSCoordSequence *coord_seq = dyn_GEOSGeom_getCoordSeq_r(handle, geom);
  if (coord_seq == NULL) {
    return SEDONA_GEOS_ERROR;
  }

  GeomBuffer geom_buf;
  SedonaErrorCode err = geom_buf_alloc(&geom_buf, POINT, srid, cs_info, 0);
  if (err != SEDONA_SUCCESS) {
    return err;
  }

  err = geom_buf_write_coords(&geom_buf, handle, coord_seq, cs_info);
  if (err != SEDONA_SUCCESS) {
    goto handle_error;
  }

  *p_buf = geom_buf.buf;
  *p_buf_size = geom_buf.buf_size;
  return SEDONA_SUCCESS;

handle_error:
  free(geom_buf.buf);
  return SEDONA_GEOS_ERROR;
}

static SedonaErrorCode sedona_deserialize_point(GEOSContextHandle_t handle,
                                                int srid, GeomBuffer *geom_buf,
                                                CoordinateSequenceInfo *cs_info,
                                                GEOSGeometry **p_geom) {
  GEOSCoordSequence *coord_seq = NULL;
  GEOSGeometry *geom = NULL;
  if (cs_info->num_coords == 0) {
    geom = dyn_GEOSGeom_createEmptyPoint_r(handle);
  } else if (cs_info->dims == 2) {
    double x = *geom_buf->buf_coord++;
    double y = *geom_buf->buf_coord++;
    geom = dyn_GEOSGeom_createPointFromXY_r(handle, x, y);
  } else {
    SedonaErrorCode err =
        geom_buf_read_coords(geom_buf, handle, cs_info, &coord_seq);
    if (err != SEDONA_SUCCESS) {
      return err;
    }
    geom = dyn_GEOSGeom_createPoint_r(handle, coord_seq);
  }

  if (geom != NULL) {
    *p_geom = geom;
    return SEDONA_SUCCESS;
  } else {
    if (coord_seq != NULL) {
      dyn_GEOSCoordSeq_destroy_r(handle, coord_seq);
    }
    return SEDONA_GEOS_ERROR;
  }
}

static SedonaErrorCode sedona_serialize_linestring(
    GEOSContextHandle_t handle, const GEOSGeometry *geom, int srid,
    CoordinateSequenceInfo *cs_info, char **p_buf, int *p_buf_size) {
  const GEOSCoordSequence *coord_seq = dyn_GEOSGeom_getCoordSeq_r(handle, geom);
  if (coord_seq == NULL) {
    return SEDONA_GEOS_ERROR;
  }

  GeomBuffer geom_buf;
  SedonaErrorCode err = geom_buf_alloc(&geom_buf, LINESTRING, srid, cs_info, 0);
  if (err != SEDONA_SUCCESS) {
    return err;
  }

  err = geom_buf_write_coords(&geom_buf, handle, coord_seq, cs_info);
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
  GEOSCoordSequence *coord_seq = NULL;
  SedonaErrorCode err =
      geom_buf_read_coords(geom_buf, handle, cs_info, &coord_seq);
  if (err != SEDONA_SUCCESS) {
    return err;
  }

  GEOSGeometry *geom = dyn_GEOSGeom_createLineString_r(handle, coord_seq);
  if (geom == NULL) {
    dyn_GEOSCoordSeq_destroy_r(handle, coord_seq);
    return SEDONA_GEOS_ERROR;
  }

  *p_geom = geom;
  return SEDONA_SUCCESS;
}

static SedonaErrorCode sedona_serialize_polygon(GEOSContextHandle_t handle,
                                                const GEOSGeometry *geom,
                                                int srid,
                                                CoordinateSequenceInfo *cs_info,
                                                char **p_buf, int *p_buf_size) {
  if (cs_info->num_coords == 0) {
    RETURN_BUFFER_FOR_EMPTY_GEOM(POLYGON, cs_info->coord_type, srid);
  }

  int num_interior_rings = dyn_GEOSGetNumInteriorRings_r(handle, geom);
  if (num_interior_rings == -1) {
    return SEDONA_GEOS_ERROR;
  }

  GeomBuffer geom_buf;
  int num_rings = num_interior_rings + 1;
  SedonaErrorCode err =
      geom_buf_alloc(&geom_buf, POLYGON, srid, cs_info, num_rings + 1);
  if (err != SEDONA_SUCCESS) {
    return err;
  }

  err = geom_buf_write_polygon(&geom_buf, handle, geom, cs_info);
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
  return geom_buf_read_polygon(geom_buf, handle, cs_info, p_geom);
}

static SedonaErrorCode sedona_serialize_multipoint(
    GEOSContextHandle_t handle, const GEOSGeometry *geom, int srid,
    CoordinateSequenceInfo *cs_info, char **p_buf, int *p_buf_size) {
  int num_points = dyn_GEOSGetNumGeometries_r(handle, geom);
  if (num_points == -1) {
    return SEDONA_GEOS_ERROR;
  }

  GeomBuffer geom_buf;
  SedonaErrorCode err = geom_buf_alloc(&geom_buf, MULTIPOINT, srid, cs_info, 0);
  if (err != SEDONA_SUCCESS) {
    return err;
  }

  for (int k = 0; k < num_points; k++) {
    const GEOSGeometry *point = dyn_GEOSGetGeometryN_r(handle, geom, k);
    if (point == NULL) {
      err = SEDONA_GEOS_ERROR;
      goto handle_error;
    }

    const GEOSCoordSequence *coord_seq =
        dyn_GEOSGeom_getCoordSeq_r(handle, point);
    if (coord_seq == NULL) {
      err = SEDONA_GEOS_ERROR;
      goto handle_error;
    }

    double *prev_buf = geom_buf.buf_coord;
    err = geom_buf_write_coords(&geom_buf, handle, coord_seq, cs_info);
    if (err != SEDONA_SUCCESS) {
      goto handle_error;
    }
    if (geom_buf.buf_coord == prev_buf) {
      /* point is empty, we have to manually write NaNs */
      *geom_buf.buf_coord++ = NAN;
      *geom_buf.buf_coord++ = NAN;
      if (cs_info->has_z) {
        *geom_buf.buf_coord++ = NAN;
      }
    }
  }

  *p_buf = geom_buf.buf;
  *p_buf_size = geom_buf.buf_size;
  return SEDONA_SUCCESS;

handle_error:
  free(geom_buf.buf);
  return err;
}

static SedonaErrorCode sedona_deserialize_multipoint(
    GEOSContextHandle_t handle, int srid, GeomBuffer *geom_buf,
    CoordinateSequenceInfo *cs_info, GEOSGeometry **p_geom) {
  int num_points = cs_info->num_coords;
  GEOSGeometry **points = calloc(num_points, sizeof(GEOSGeometry *));
  if (points == NULL) {
    return SEDONA_ALLOC_ERROR;
  }

  SedonaErrorCode err = SEDONA_SUCCESS;
  for (int k = 0; k < num_points; k++) {
    GEOSGeometry *point = NULL;
    if (cs_info->has_z) {
      GEOSCoordSequence *coord_seq = NULL;
      err = geom_buf_read_coords(geom_buf, handle, cs_info, &coord_seq);
      if (err != SEDONA_SUCCESS) {
        goto handle_error;
      }
      point = dyn_GEOSGeom_createPoint_r(handle, coord_seq);
      if (point == NULL) {
        dyn_GEOSCoordSeq_destroy_r(handle, coord_seq);
        err = SEDONA_GEOS_ERROR;
        goto handle_error;
      }
    } else {
      double x = *geom_buf->buf_coord++;
      double y = *geom_buf->buf_coord++;
      /* x and y will be NaN when serialized point was an empty point. GEOS
       * will treat point with Nan ordinates as an empty point so we don't need
       * to handle NaN specially. */
      point = dyn_GEOSGeom_createPointFromXY_r(handle, x, y);
      if (point == NULL) {
        err = SEDONA_GEOS_ERROR;
        goto handle_error;
      }
    }
    points[k] = point;
  }

  GEOSGeometry *geom = dyn_GEOSGeom_createCollection_r(handle, GEOS_MULTIPOINT,
                                                       points, num_points);
  if (geom == NULL) {
    err = SEDONA_GEOS_ERROR;
    goto handle_error;
  }

  *p_geom = geom;
  return SEDONA_SUCCESS;

handle_error:
  destroy_geometry_array(handle, points, num_points);
  return err;
}

static SedonaErrorCode sedona_serialize_multilinestring(
    GEOSContextHandle_t handle, const GEOSGeometry *geom, int srid,
    CoordinateSequenceInfo *cs_info, char **p_buf, int *p_buf_size) {
  return SEDONA_SUCCESS;
}

static SedonaErrorCode sedona_deserialize_multilinestring(
    GEOSContextHandle_t handle, int srid, GeomBuffer *geom_buf,
    CoordinateSequenceInfo *cs_info, GEOSGeometry **p_geom) {
  return SEDONA_UNKNOWN_GEOM_TYPE;
}

static SedonaErrorCode sedona_serialize_multipolygon(
    GEOSContextHandle_t handle, const GEOSGeometry *geom, int srid,
    CoordinateSequenceInfo *cs_info, char **p_buf, int *p_buf_size) {
  return SEDONA_SUCCESS;
}

static SedonaErrorCode sedona_deserialize_multipolygon(
    GEOSContextHandle_t handle, int srid, GeomBuffer *geom_buf,
    CoordinateSequenceInfo *cs_info, GEOSGeometry **p_geom) {
  return SEDONA_UNKNOWN_GEOM_TYPE;
}

static inline int aligned_offset(int offset) { return (offset + 7) & ~7; }

static SedonaErrorCode sedona_serialize_geometrycollection(
    GEOSContextHandle_t handle, const GEOSGeometry *geom, int srid,
    char **p_buf, int *p_buf_size) {
  int num_geoms = dyn_GEOSGetNumGeometries_r(handle, geom);
  if (num_geoms == -1) {
    return SEDONA_GEOS_ERROR;
  }
  int total_size = 8;
  void *scratch = calloc(num_geoms, sizeof(char *) + sizeof(int));
  if (scratch == NULL) {
    return SEDONA_ALLOC_ERROR;
  }
  char **geom_bufs = scratch;
  int *geom_buf_sizes = scratch + num_geoms * sizeof(char *);
  SedonaErrorCode err = SEDONA_SUCCESS;

  /* Serialize geometries individually */
  for (int k = 0; k < num_geoms; k++) {
    const GEOSGeometry *child_geom = dyn_GEOSGetGeometryN_r(handle, geom, k);
    if (child_geom == NULL) {
      err = SEDONA_GEOS_ERROR;
      goto handle_error;
    }

    char *buf = NULL;
    int buf_size = 0;
    err = sedona_serialize_geom(handle, child_geom, &buf, &buf_size);
    if (err != SEDONA_SUCCESS) {
      goto handle_error;
    }
    geom_bufs[k] = buf;
    geom_buf_sizes[k] = buf_size;
    total_size += aligned_offset(buf_size);
  }

  char *buf = alloc_buffer_for_geom(GEOMETRYCOLLECTION, XY, srid, total_size,
                                    num_geoms);
  if (buf == NULL) {
    err = SEDONA_ALLOC_ERROR;
    goto handle_error;
  }

  char *p_next_geom = buf + 8;
  for (int k = 0; k < num_geoms; k++) {
    int buf_size = geom_buf_sizes[k];
    memcpy(p_next_geom, geom_bufs[k], buf_size);
    free(geom_bufs[k]);
    int padding_size = aligned_offset(buf_size) - buf_size;
    p_next_geom += buf_size;
    if (padding_size > 0) {
      memset(p_next_geom, 0, padding_size);
      p_next_geom += padding_size;
    }
  }

  free(scratch);
  *p_buf = buf;
  *p_buf_size = total_size;
  return SEDONA_SUCCESS;

handle_error:
  for (int k = 0; k < num_geoms; k++) {
    free(geom_bufs[k]);
  }
  free(scratch);
  return err;
}

static SedonaErrorCode deserialize_geom_buf(GEOSContextHandle_t handle,
                                            GeometryTypeId geom_type_id,
                                            int srid, GeomBuffer *geom_buf,
                                            CoordinateSequenceInfo *cs_info,
                                            GEOSGeometry **p_geom);

static SedonaErrorCode sedona_deserialize_geometrycollection(
    GEOSContextHandle_t handle, int srid, GeomBuffer *geom_buf,
    CoordinateSequenceInfo *cs_info, GEOSGeometry **p_geom) {
  SedonaErrorCode err = SEDONA_SUCCESS;
  int num_geoms = cs_info->num_coords;
  GEOSGeometry **child_geoms = calloc(num_geoms, sizeof(GEOSGeometry *));
  if (child_geoms == NULL) {
    return SEDONA_ALLOC_ERROR;
  }

  const char *buf = (const char *)geom_buf->buf + 8;
  int remaining_size = geom_buf->buf_size - 8;
  for (int k = 0; k < num_geoms; k++) {
    GeomBuffer child_geom_buf;
    GeometryTypeId geom_type_id;
    int child_srid;
    err = read_geom_buf_header(buf, remaining_size, &child_geom_buf, cs_info,
                               &geom_type_id, &child_srid);
    if (err != SEDONA_SUCCESS) {
      goto handle_error;
    }

    GEOSGeometry *child_geom = NULL;
    err = deserialize_geom_buf(handle, geom_type_id, child_srid,
                               &child_geom_buf, cs_info, &child_geom);
    if (err != SEDONA_SUCCESS) {
      goto handle_error;
    }

    child_geoms[k] = child_geom;

    /* geom_buf.buf_int works as the marker for the end address of the geometry
     * buffer */
    const char *end_of_geom = (const char *)child_geom_buf.buf_int;
    int bytes_read = aligned_offset(end_of_geom - buf);
    remaining_size -= bytes_read;
    buf += bytes_read;
  }

  GEOSGeometry *geom_collection = dyn_GEOSGeom_createCollection_r(
      handle, GEOS_GEOMETRYCOLLECTION, child_geoms, num_geoms);
  if (geom_collection == NULL) {
    err = SEDONA_GEOS_ERROR;
    goto handle_error;
  }

  free(child_geoms);
  *p_geom = geom_collection;
  geom_buf->buf_int = (int *)buf;
  return SEDONA_SUCCESS;

handle_error:
  destroy_geometry_array(handle, child_geoms, num_geoms);
  return err;
}

SedonaErrorCode sedona_serialize_geom(GEOSContextHandle_t handle,
                                      const GEOSGeometry *geom, char **p_buf,
                                      int *p_buf_size) {
  int srid = dyn_GEOSGetSRID_r(handle, geom);
  int geom_type_id = dyn_GEOSGeomTypeId_r(handle, geom);
  if (geom_type_id == GEOS_GEOMETRYCOLLECTION) {
    return sedona_serialize_geometrycollection(handle, geom, srid, p_buf,
                                               p_buf_size);
  }

  CoordinateSequenceInfo cs_info;
  SedonaErrorCode err = get_coord_seq_info_from_geom(handle, geom, &cs_info);
  if (err != SEDONA_SUCCESS) {
    return err;
  }

  switch (geom_type_id) {
    case GEOS_POINT:
      err = sedona_serialize_point(handle, geom, srid, &cs_info, p_buf,
                                   p_buf_size);
      break;
    case GEOS_LINESTRING:
      err = sedona_serialize_linestring(handle, geom, srid, &cs_info, p_buf,
                                        p_buf_size);
      break;
    case GEOS_LINEARRING:
      err = SEDONA_UNSUPPORTED_GEOM_TYPE;
      break;
    case GEOS_POLYGON:
      err = sedona_serialize_polygon(handle, geom, srid, &cs_info, p_buf,
                                     p_buf_size);
      break;
    case GEOS_MULTIPOINT:
      err = sedona_serialize_multipoint(handle, geom, srid, &cs_info, p_buf,
                                        p_buf_size);
      break;
    case GEOS_MULTILINESTRING:
      err = sedona_serialize_multilinestring(handle, geom, srid, &cs_info,
                                             p_buf, p_buf_size);
      break;
    case GEOS_MULTIPOLYGON:
      err = sedona_serialize_multipolygon(handle, geom, srid, &cs_info, p_buf,
                                          p_buf_size);
      break;
    default:
      err = SEDONA_UNKNOWN_GEOM_TYPE;
  }

  return err;
}

static SedonaErrorCode deserialize_geom_buf(GEOSContextHandle_t handle,
                                            GeometryTypeId geom_type_id,
                                            int srid, GeomBuffer *geom_buf,
                                            CoordinateSequenceInfo *cs_info,
                                            GEOSGeometry **p_geom) {
  SedonaErrorCode err = SEDONA_SUCCESS;
  switch (geom_type_id) {
    case POINT:
      err = sedona_deserialize_point(handle, srid, geom_buf, cs_info, p_geom);
      break;
    case LINESTRING:
      err = sedona_deserialize_linestring(handle, srid, geom_buf, cs_info,
                                          p_geom);
      break;
    case POLYGON:
      err = sedona_deserialize_polygon(handle, srid, geom_buf, cs_info, p_geom);
      break;
    case MULTIPOINT:
      err = sedona_deserialize_multipoint(handle, srid, geom_buf, cs_info,
                                          p_geom);
      break;
    case MULTILINESTRING:
      err = sedona_deserialize_multilinestring(handle, srid, geom_buf, cs_info,
                                               p_geom);
      break;
    case MULTIPOLYGON:
      err = sedona_deserialize_multipolygon(handle, srid, geom_buf, cs_info,
                                            p_geom);
      break;
    case GEOMETRYCOLLECTION:
      err = sedona_deserialize_geometrycollection(handle, srid, geom_buf,
                                                  cs_info, p_geom);
      break;
    default:
      return SEDONA_UNSUPPORTED_GEOM_TYPE;
  }

  if (err != SEDONA_SUCCESS) {
    return err;
  }
  if (srid != 0) {
    dyn_GEOSSetSRID_r(handle, *p_geom, srid);
  }
  return SEDONA_SUCCESS;
}

SedonaErrorCode sedona_deserialize_geom(GEOSContextHandle_t handle,
                                        const char *buf, int buf_size,
                                        GEOSGeometry **p_geom) {
  GeomBuffer geom_buf;
  CoordinateSequenceInfo cs_info;
  GeometryTypeId geom_type_id;
  int srid;
  SedonaErrorCode err = read_geom_buf_header(buf, buf_size, &geom_buf, &cs_info,
                                             &geom_type_id, &srid);
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
    case SEDONA_INCOMPLETE_BUFFER:
      return "Buffer to be deserialized is incomplete";
    case SEDONA_BAD_GEOM_BUFFER:
      return "Bad serialized geometry buffer";
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
