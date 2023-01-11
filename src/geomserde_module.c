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

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "geomserde.h"
#include "pygeos/c_api.h"
#include "geos_c_nodeps.h"

PyDoc_STRVAR(module_doc,
             "Geometry serialization module for GeometryType in PySpark SQL.");

static PyObject *load_libgeos_c(PyObject *self, PyObject *args) {
  const char *libgeos_c_path = NULL;
  if (!PyArg_ParseTuple(args, "z", &libgeos_c_path)) {
    return NULL;
  }

  char err_msg[256];
  if (load_geos_c_library(libgeos_c_path, err_msg, sizeof(err_msg)) != 0) {
    PyErr_Format(PyExc_RuntimeError, "Failed to find libgeos_c functions: %s", err_msg);
    return NULL;
  }

  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject *serialize(PyObject *self, PyObject *args) {
  PyObject *pygeos_geom = NULL;
  if (!PyArg_ParseTuple(args, "O", &pygeos_geom)) {
    return NULL;
  }

  GEOSGeometry *geos_geom = NULL;
  char success = PyGEOS_GetGEOSGeometry(pygeos_geom, &geos_geom);
  if (success == 0) {
    PyErr_SetString(
        PyExc_TypeError,
        "Argument is of incorrect type. Please provide only Geometry objects.");
    return NULL;
  }
  if (geos_geom == NULL) {
    Py_INCREF(Py_None);
    return Py_None;
  }

  char *buf = NULL;
  int buf_size = 0;
  GEOSContextHandle_t handle = pf_GEOS_init_r();
  int err = sedona_serialize_geom(geos_geom, handle, &buf, &buf_size);
  pf_GEOS_finish_r(handle);
  if (err != SEDONA_SUCCESS) {
    const char *errmsg = sedona_get_error_message(err);
    PyErr_SetString(PyExc_ValueError, errmsg);
    return NULL;
  }

  PyObject *bytearray = PyByteArray_FromStringAndSize(buf, buf_size);
  free(buf);
  return bytearray;
}

static PyObject *deserialize(PyObject *self, PyObject *args) {
  Py_buffer view;
  if (!PyArg_ParseTuple(args, "y*", &view)) {
    return NULL;
  }

  /* The Py_buffer filled by PyArg_ParseTuple is guaranteed to be C-contiguous,
   * so we can simply proceed with view.buf and view.len */
  GEOSGeometry *geom = NULL;
  GEOSContextHandle_t handle = pf_GEOS_init_r();
  const char *buf = view.buf;
  int buf_size = view.len;
  int err = sedona_deserialize_geom(buf, buf_size, handle, &geom);
  PyBuffer_Release(&view);
  if (err != SEDONA_SUCCESS) {
    pf_GEOS_finish_r(handle);
    const char *errmsg = sedona_get_error_message(err);
    PyErr_SetString(PyExc_ValueError, errmsg);
    return NULL;
  }

  PyObject *pygeom = PyGEOS_CreateGeometry(geom, handle);
  pf_GEOS_finish_r(handle);
  return pygeom;
}

static PyMethodDef geomserde_methods_shapely_2[] = {
    {"load_libgeos_c", load_libgeos_c, METH_VARARGS, "Load libgeos_c."},
    {"serialize", serialize, METH_VARARGS,
     "Serialize geometry object as bytearray."},
    {"deserialize", deserialize, METH_VARARGS,
     "Deserialize bytes-like object to geometry object."},
    {NULL, NULL, 0, NULL}, /* Sentinel */
};

static struct PyModuleDef geomserde_module_shapely_2 = {
  PyModuleDef_HEAD_INIT,
  "geomserde",
  module_doc,
  0,
  geomserde_methods_shapely_2
};


PyMODINIT_FUNC PyInit_geomserde(void) {
  if (import_shapely_c_api() != 0) {
    /* TODO: initialize geomserde_module as shapely 1.x compatible version */
    return NULL;
  }

  return PyModuleDef_Init(&geomserde_module_shapely_2);
}
