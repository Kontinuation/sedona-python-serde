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
#include "geos_c_nodeps.h"

#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <dlfcn.h>

GEOSContextHandle_t (*pf_GEOS_init_r)();
void (*pf_GEOS_finish_r)(GEOSContextHandle_t handle);
int (*pf_GEOSGeomTypeId_r)(GEOSContextHandle_t handle, const GEOSGeometry *g);
char (*pf_GEOSHasZ_r)(GEOSContextHandle_t handle, const GEOSGeometry *g);
GEOSGeometry *(*pf_GEOSGeom_createPointFromXY_r)(
    GEOSContextHandle_t handle, double x, double y);

#define LOAD_GEOS_FUNCTION(func)                                               \
  if (load_geos_c_symbol(handle, #func, (void **)&pf_##func, err_msg, len) !=  \
      0) {                                                                     \
    return -1;                                                                 \
  }

static int load_geos_c_symbol(void *handle, const char *func_name, void **pf,
                              char *err_msg, int len) {  
  void *func = dlsym(handle, func_name);
  if (func == NULL) {
    snprintf(err_msg, len, "%s", dlerror());
    return -1;
  }
  *pf = func;
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

int load_geos_c_from_handle(void *handle, char *err_msg, int len) {
  LOAD_GEOS_FUNCTION(GEOS_init_r);
  LOAD_GEOS_FUNCTION(GEOS_finish_r);
  LOAD_GEOS_FUNCTION(GEOSGeomTypeId_r);
  LOAD_GEOS_FUNCTION(GEOSHasZ_r);
  LOAD_GEOS_FUNCTION(GEOSGeom_createPointFromXY_r);
  return 0;
}
