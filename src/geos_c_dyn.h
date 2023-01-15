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

#ifndef GEOS_C_NODEPS
#define GEOS_C_NODEPS

/* We don't need to depend on geos_c.h in libgeos directly. We can add forward
 * type declarations for them since the libgeos C API only deals with pointer
 * types of them, so they are essentially opaque to us. */
struct GEOSGeom_t;
struct GEOSPrepGeom_t;
struct GEOSCoordSeq_t;
struct GEOSContextHandle;

typedef struct GEOSGeom_t GEOSGeometry;
typedef struct GEOSPrepGeom_t GEOSPreparedGeometry;
typedef struct GEOSContextHandle *GEOSContextHandle_t;
typedef struct GEOSCoordSeq_t GEOSCoordSequence;

typedef void (*GEOSMessageHandler)(const char *fmt, ...);

/**
 * Check if GEOS C was loaded

 * @return 1 if GEOS C was loaded, otherwise return 0.
 */
extern int is_geos_c_loaded();

/**
 * Load GEOS C functions from libgeos_c library on specified path.
 *
 * We don't link to libgeos_c directly because shapely already brought its own
 * copy of libgeos_c, it is better to use the same copy of libgeos_c in our
 * extension since our extension is an augmentation of shapely. Mixing various
 * versions of the same library together is likely to cause nasty bugs.
 *
 * @param path path to the libgeos_c library
 * @param err_msg buffer for receiving error message in case of errors
 * @param len length of the error message buffer
 * @return 0 when GEOS functions were loaded correctly, otherwise returns a
 * non-zero value
 */
extern int load_geos_c_library(const char *path, char *err_msg, int len);

/**
 * Load GEOS C functions from specified (platform-specific) library handle
 *
 * This function is similar to `load_geos_c_library`. The only exception is
 * that it does not load the libgeos_c library from file.
 *
 * @param handle platform-specific handle to load functions from
 * @param err_msg buffer for receiving error message in case of errors
 * @param len length of the error message buffer
 * @return 0 when GEOS functions were loaded correctly, otherwise returns a
 * non-zero value
 */
extern int load_geos_c_from_handle(void *handle, char *err_msg, int len);

/* The following are function pointers to GEOS C APIs provided by
 * libgeos_c. These functions must be called after a successful invocation of
 * `load_geos_c_functions` */

extern GEOSContextHandle_t (*dyn_GEOS_init_r)();

extern void (*dyn_GEOS_finish_r)(GEOSContextHandle_t handle);

extern GEOSMessageHandler (*dyn_GEOSContext_setErrorHandler_r)(
    GEOSContextHandle_t extHandle, GEOSMessageHandler ef);

extern int (*dyn_GEOSGeomTypeId_r)(GEOSContextHandle_t handle,
                                   const GEOSGeometry *g);

extern char (*dyn_GEOSHasZ_r)(GEOSContextHandle_t handle,
                              const GEOSGeometry *g);

extern int (*dyn_GEOSGetSRID_r)(GEOSContextHandle_t handle,
                                const GEOSGeometry *g);

extern void (*dyn_GEOSSetSRID_r)(GEOSContextHandle_t handle, GEOSGeometry *g,
                                 int SRID);

extern const GEOSCoordSequence *(*dyn_GEOSGeom_getCoordSeq_r)(
    GEOSContextHandle_t handle, const GEOSGeometry *g);

extern int (*dyn_GEOSCoordSeq_getDimensions_r)(GEOSContextHandle_t handle,
                                               const GEOSCoordSequence *s,
                                               unsigned int *dims);

extern int (*dyn_GEOSCoordSeq_getSize_r)(GEOSContextHandle_t handle,
                                         const GEOSCoordSequence *s,
                                         unsigned int *size);

extern GEOSCoordSequence *(*dyn_GEOSCoordSeq_copyFromBuffer_r)(
    GEOSContextHandle_t handle, const double *buf, unsigned int size, int hasZ,
    int hasM);

extern int (*dyn_GEOSCoordSeq_copyToBuffer_r)(GEOSContextHandle_t handle,
                                              const GEOSCoordSequence *s,
                                              double *buf, int hasZ, int hasM);

extern int (*dyn_GEOSCoordSeq_getXY_r)(GEOSContextHandle_t handle,
                                       const GEOSCoordSequence *s,
                                       unsigned int idx, double *x, double *y);

extern int (*dyn_GEOSCoordSeq_getXYZ_r)(GEOSContextHandle_t handle,
                                        const GEOSCoordSequence *s,
                                        unsigned int idx, double *x, double *y,
                                        double *z);

extern int (*dyn_GEOSCoordSeq_setXY_r)(GEOSContextHandle_t handle,
                                       GEOSCoordSequence *s, unsigned int idx,
                                       double x, double y);

extern int (*dyn_GEOSCoordSeq_setXYZ_r)(GEOSContextHandle_t handle,
                                        GEOSCoordSequence *s, unsigned int idx,
                                        double x, double y, double z);

extern const GEOSGeometry *(*dyn_GEOSGetExteriorRing_r)(
    GEOSContextHandle_t handle, const GEOSGeometry *g);

extern int (*dyn_GEOSGetNumInteriorRings_r)(GEOSContextHandle_t handle,
                                            const GEOSGeometry *g);

extern int (*dyn_GEOSGetNumCoordinates_r)(GEOSContextHandle_t handle,
                                          const GEOSGeometry *g);

extern int (*dyn_GEOSGeom_getCoordinateDimension_r)(GEOSContextHandle_t handle,
                                                    const GEOSGeometry *g);

extern const GEOSGeometry *(*dyn_GEOSGetInteriorRingN_r)(
    GEOSContextHandle_t handle, const GEOSGeometry *g, int n);

extern GEOSGeometry *(*dyn_GEOSGeom_createPointFromXY_r)(
    GEOSContextHandle_t handle, double x, double y);

/* Supported geometry types */
enum GEOSGeomTypes {
  GEOS_POINT,
  GEOS_LINESTRING,
  GEOS_LINEARRING,
  GEOS_POLYGON,
  GEOS_MULTIPOINT,
  GEOS_MULTILINESTRING,
  GEOS_MULTIPOLYGON,
  GEOS_GEOMETRYCOLLECTION
};

#endif /* GEOS_C_NODEPS */
