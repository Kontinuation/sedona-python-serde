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

const char *sedona_get_error_message(int err) {
  switch (err) {
    case SEDONA_SUCCESS:
      return "";
    default:
      return "Unknown failure occurred";
  }
}

int sedona_serialize_geom(GEOSGeometry *geom, GEOSContextHandle_t handle,
                          char **p_buf, int *p_buf_size) {  
  int buf_size = 8;
  char *buf = malloc(buf_size);
  for (int k = 0; k < buf_size; k++) {
    buf[k] = '0' + k;
  }

  *p_buf = buf;
  *p_buf_size = buf_size;
  return SEDONA_SUCCESS;
}

int sedona_deserialize_geom(const char *buf, int buf_size,
                            GEOSContextHandle_t handle, GEOSGeometry **p_geom) {  
  GEOSGeometry *geom = pf_GEOSGeom_createPointFromXY_r(handle, 10.0, 20.0);
  *p_geom = geom;
  return SEDONA_SUCCESS;
}
