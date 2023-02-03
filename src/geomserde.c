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

#include "geom_buf.h"
#include "geos_c_dyn.h"

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

  GeomBuffer geom_buf;
  err = geom_buf_alloc(&geom_buf, POINT, srid, &cs_info, 0);
  if (err != SEDONA_SUCCESS) {
    return err;
  }

  err = geom_buf_write_coords(&geom_buf, handle, coord_seq, &cs_info);
  if (err != SEDONA_SUCCESS) {
    goto geos_error;
  }

  *p_buf = geom_buf.buf;
  *p_buf_size = geom_buf.buf_size;
  return SEDONA_SUCCESS;

geos_error:
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
    double x = geom_buf->buf_coord[0];
    double y = geom_buf->buf_coord[1];
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
  return geom_buf_read_polygon(geom_buf, handle, cs_info, p_geom);
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
    case SEDONA_INVALID_ARG_ERROR:
      return "Invalid argument";
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
