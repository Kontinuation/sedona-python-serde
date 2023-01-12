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

#define _GNU_SOURCE
#include "geos_c_dyn.h"

#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <dlfcn.h>

GEOSContextHandle_t (*dyn_GEOS_init_r)();

void (*dyn_GEOS_finish_r)(GEOSContextHandle_t handle);

GEOSMessageHandler (*dyn_GEOSContext_setErrorHandler_r)(
    GEOSContextHandle_t extHandle, GEOSMessageHandler ef);

int (*dyn_GEOSGeomTypeId_r)(GEOSContextHandle_t handle, const GEOSGeometry *g);

char (*dyn_GEOSHasZ_r)(GEOSContextHandle_t handle, const GEOSGeometry *g);

int (*dyn_GEOSGetSRID_r)(GEOSContextHandle_t handle, const GEOSGeometry *g);

void (*dyn_GEOSSetSRID_r)(GEOSContextHandle_t handle, GEOSGeometry *g,
                          int SRID);

const GEOSCoordSequence *(*dyn_GEOSGeom_getCoordSeq_r)(
    GEOSContextHandle_t handle, const GEOSGeometry *g);

int (*dyn_GEOSCoordSeq_getDimensions_r)(GEOSContextHandle_t handle,
                                        const GEOSCoordSequence *s,
                                        unsigned int *dims);

int (*dyn_GEOSCoordSeq_getSize_r)(GEOSContextHandle_t handle,
                                  const GEOSCoordSequence *s,
                                  unsigned int *size);

GEOSCoordSequence *(*dyn_GEOSCoordSeq_copyFromBuffer_r)(
    GEOSContextHandle_t handle, const double *buf, unsigned int size, int hasZ,
    int hasM);

int (*dyn_GEOSCoordSeq_copyToBuffer_r)(GEOSContextHandle_t handle,
                                       const GEOSCoordSequence *s, double *buf,
                                       int hasZ, int hasM);

int (*dyn_GEOSCoordSeq_getXY_r)(GEOSContextHandle_t handle,
                                const GEOSCoordSequence *s, unsigned int idx,
                                double *x, double *y);

int (*dyn_GEOSCoordSeq_getXYZ_r)(GEOSContextHandle_t handle,
                                 const GEOSCoordSequence *s, unsigned int idx,
                                 double *x, double *y, double *z);

int (*dyn_GEOSCoordSeq_setXY_r)(GEOSContextHandle_t handle,
                                GEOSCoordSequence *s, unsigned int idx,
                                double x, double y);

int (*dyn_GEOSCoordSeq_setXYZ_r)(GEOSContextHandle_t handle,
                                 GEOSCoordSequence *s, unsigned int idx,
                                 double x, double y, double z);

GEOSGeometry *(*dyn_GEOSGeom_createPointFromXY_r)(GEOSContextHandle_t handle,
                                                  double x, double y);

#define LOAD_GEOS_FUNCTION(func)                                               \
  if (load_geos_c_symbol(handle, #func, (void **)&dyn_##func, err_msg, len) !=  \
      0) {                                                                     \
    return -1;                                                                 \
  }

static int load_geos_c_symbol(void *handle, const char *func_name, void **p_func,
                              char *err_msg, int len) {  
  void *func = dlsym(handle, func_name);
  if (func == NULL) {
    snprintf(err_msg, len, "%s", dlerror());
    return -1;
  }
  *p_func = func;
  return 0;
}

int load_geos_c_library(const char *path, char *err_msg, int len) {
  void *handle = dlopen(path, RTLD_LOCAL | RTLD_NOW);
  if (handle == NULL) {
    snprintf(err_msg, len, "%s", dlerror());
    return -1;
  }
  return load_geos_c_from_handle(handle, err_msg, len);
}

int is_geos_c_loaded() {
  return (dyn_GEOS_init_r != NULL)? 1: 0;
}

int load_geos_c_from_handle(void *handle, char *err_msg, int len) {
  LOAD_GEOS_FUNCTION(GEOS_finish_r);  
  LOAD_GEOS_FUNCTION(GEOSContext_setErrorHandler_r);  
  LOAD_GEOS_FUNCTION(GEOSGeomTypeId_r);
  LOAD_GEOS_FUNCTION(GEOSHasZ_r);
  LOAD_GEOS_FUNCTION(GEOSGetSRID_r);
  LOAD_GEOS_FUNCTION(GEOSSetSRID_r);
  LOAD_GEOS_FUNCTION(GEOSGeom_getCoordSeq_r);
  LOAD_GEOS_FUNCTION(GEOSCoordSeq_getDimensions_r);
  LOAD_GEOS_FUNCTION(GEOSCoordSeq_getSize_r);
  LOAD_GEOS_FUNCTION(GEOSCoordSeq_getXY_r);
  LOAD_GEOS_FUNCTION(GEOSCoordSeq_getXYZ_r);
  LOAD_GEOS_FUNCTION(GEOSCoordSeq_setXY_r);
  LOAD_GEOS_FUNCTION(GEOSCoordSeq_setXYZ_r);
  LOAD_GEOS_FUNCTION(GEOSGeom_createPointFromXY_r);

  /* These functions are not mandantory, only libgeos (>=3.10.0) bundled with
   * shapely>=1.8.0 has these functions. */
  dyn_GEOSCoordSeq_copyFromBuffer_r =
      dlsym(handle, "GEOSCoordSeq_copyFromBuffer_r");
  dyn_GEOSCoordSeq_copyToBuffer_r =
      dlsym(handle, "GEOSCoordSeq_copyToBuffer_r");  

  /* Deliberately load GEOS_init_r after all other functions, so that we can
   * check if all functions were loaded by checking if GEOS_init_r was
   * loaded. */
  LOAD_GEOS_FUNCTION(GEOS_init_r);
  return 0;
}
