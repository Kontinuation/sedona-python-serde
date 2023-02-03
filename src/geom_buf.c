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

#include "geom_buf.h"

#include "geomserde.h"
#include "geos_c_dyn.h"

SedonaErrorCode get_coord_seq_info(GEOSContextHandle_t handle,
                                   const GEOSCoordSequence *coord_seq,
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

SedonaErrorCode get_coord_seq_info_from_geom(
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

void *alloc_buffer_for_geom(GeometryTypeId geom_type_id,
                            CoordinateType coord_type, int srid, int buf_size,
                            int num_coords) {
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
    return SEDONA_GEOS_ERROR;
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

static SedonaErrorCode copy_buffer_to_coord_seq(
    GEOSContextHandle_t handle, double *buf, int num_coords, int has_z,
    int has_m, GEOSCoordSequence **p_coord_seq) {
  if (dyn_GEOSCoordSeq_copyFromBuffer_r != NULL) {
    GEOSCoordSequence *coord_seq = dyn_GEOSCoordSeq_copyFromBuffer_r(
        handle, buf, num_coords, has_z, has_m);
    if (coord_seq == NULL) {
      return SEDONA_GEOS_ERROR;
    }
    *p_coord_seq = coord_seq;
    return SEDONA_SUCCESS;
  }

  /* slow path for old libgeos */
  GEOSCoordSequence *coord_seq =
      dyn_GEOSCoordSeq_create_r(handle, num_coords, has_z ? 3 : 2);
  if (coord_seq == NULL) {
    return SEDONA_GEOS_ERROR;
  }
  for (int k = 0; k < num_coords; k++) {
    if (has_z) {
      double x = *buf++;
      double y = *buf++;
      double z = *buf++;
      if (dyn_GEOSCoordSeq_setXYZ_r(handle, coord_seq, k, x, y, z) == 0) {
        dyn_GEOSCoordSeq_destroy_r(handle, coord_seq);
        return SEDONA_GEOS_ERROR;
      }
    } else {
      double x = *buf++;
      double y = *buf++;
      if (dyn_GEOSCoordSeq_setXY_r(handle, coord_seq, k, x, y) == 0) {
        dyn_GEOSCoordSeq_destroy_r(handle, coord_seq);
        return SEDONA_GEOS_ERROR;
      }
    }
  }

  *p_coord_seq = coord_seq;
  return SEDONA_SUCCESS;
}

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

SedonaErrorCode geom_buf_alloc(GeomBuffer *geom_buf,
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

SedonaErrorCode parse_geom_buf_header(const char *buf, int buf_size,
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
  if (geom_type_id != GEOMETRYCOLLECTION) {
    if (8 + num_coords * bytes_per_coord > buf_size) {
      return SEDONA_INCOMPLETE_BUFFER;
    }

    int dims = (coord_type == XYZ ? 3 : 2);
    int has_z = (coord_type == XYZ ? 1 : 0);
    int has_m = 0;
    cs_info->bytes_per_coord = bytes_per_coord;
    cs_info->coord_type = coord_type;
    cs_info->num_coords = num_coords;
    cs_info->dims = dims;
    cs_info->has_z = has_z;
    cs_info->has_m = has_m;

    geom_buf->buf = (void *)buf;
    geom_buf->buf_coord = (double *)(buf + 8);
    geom_buf->buf_coord_end = geom_buf->buf_coord + num_coords * dims;
    geom_buf->buf_int = (int *)geom_buf->buf_coord_end;
    geom_buf->buf_int_end = (int *)(buf + buf_size);
    geom_buf->buf_size = buf_size;
  } else {
    /* num_coords for GeometryCollection is number of geometries in the
     * collection, other fields in cs_info are unused. */
    cs_info->num_coords = num_coords;

    /* geom_buf contains a series of serialized geometries. buf_coord is the
     * begining of its first child geometry, and buf_int is unused. */
    const char *buf_end = buf + buf_size;
    geom_buf->buf = (void *)buf;
    geom_buf->buf_coord = (double *)(buf + 8);
    geom_buf->buf_coord_end = (double *)buf_end;
    geom_buf->buf_int = (int *)buf_end;
    geom_buf->buf_int_end = (int *)buf_end;
    geom_buf->buf_size = buf_size;
  }

  *p_geom_type_id = geom_type_id;
  *p_srid = srid;
  return SEDONA_SUCCESS;
}

SedonaErrorCode geom_buf_write_coords(GeomBuffer *geom_buf,
                                      GEOSContextHandle_t handle,
                                      const GEOSCoordSequence *coord_seq,
                                      const CoordinateSequenceInfo *cs_info) {
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

SedonaErrorCode geom_buf_read_coords(GeomBuffer *geom_buf,
                                     GEOSContextHandle_t handle,
                                     const CoordinateSequenceInfo *cs_info,
                                     GEOSCoordSequence **p_coord_seq) {
  int num_coords = cs_info->num_coords;
  int dims = cs_info->dims;
  int num_ordinates = num_coords * dims;
  if (geom_buf->buf_coord + num_ordinates > geom_buf->buf_coord_end) {
    return SEDONA_INCOMPLETE_BUFFER;
  }
  int err =
      copy_buffer_to_coord_seq(handle, geom_buf->buf_coord, num_coords,
                               cs_info->has_z, cs_info->has_m, p_coord_seq);
  if (err != SEDONA_SUCCESS) {
    return err;
  }
  geom_buf->buf_coord += num_ordinates;
  return SEDONA_SUCCESS;
}

SedonaErrorCode geom_buf_write_polygon(GeomBuffer *geom_buf,
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
