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
  PyObject *obj;
  char err_msg[256];
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }

  if (PyLong_Check(obj)) {
    long handle = PyLong_AsLong(obj);
    if (load_geos_c_from_handle((void *)handle, err_msg, sizeof(err_msg)) !=
        0) {
      PyErr_Format(PyExc_RuntimeError, "Failed to find libgeos_c functions: %s",
                   err_msg);
      return NULL;
    }
  } else if (PyUnicode_Check(obj)) {
    const char *libname = PyUnicode_AsUTF8(obj);
    if (load_geos_c_library(libname, err_msg, sizeof(err_msg)) != 0) {
      PyErr_Format(PyExc_RuntimeError, "Failed to find libgeos_c functions: %s",
                   err_msg);
      return NULL;
    }
  } else {
    PyErr_SetString(PyExc_TypeError,
                    "load_libgeos_c expects a string or long argument");
    return NULL;
  }

  Py_INCREF(Py_None);
  return Py_None;
}

static GEOSContextHandle_t get_thread_local_geos_context_handle() {
  static _Thread_local GEOSContextHandle_t handle;
  if (handle == NULL) {
    handle = pf_GEOS_init_r();
  }
  return handle;
}

static PyObject *do_serialize(GEOSGeometry *geos_geom) {
  if (geos_geom == NULL) {
    Py_INCREF(Py_None);
    return Py_None;
  }

  char *buf = NULL;
  int buf_size = 0;
  GEOSContextHandle_t handle = get_thread_local_geos_context_handle();
  int err = sedona_serialize_geom(geos_geom, handle, &buf, &buf_size);
  if (err != SEDONA_SUCCESS) {
    const char *errmsg = sedona_get_error_message(err);
    PyErr_SetString(PyExc_ValueError, errmsg);
    return NULL;
  }

  PyObject *bytearray = PyByteArray_FromStringAndSize(buf, buf_size);
  free(buf);
  return bytearray;
}

static GEOSGeometry *do_deserialize(PyObject *args,
                                    GEOSContextHandle_t *out_handle) {
  Py_buffer view;
  if (!PyArg_ParseTuple(args, "y*", &view)) {
    return NULL;
  }

  /* The Py_buffer filled by PyArg_ParseTuple is guaranteed to be C-contiguous,
   * so we can simply proceed with view.buf and view.len */
  const char *buf = view.buf;
  int buf_size = view.len;
  GEOSGeometry *geom = NULL;
  GEOSContextHandle_t handle = get_thread_local_geos_context_handle();
  int err = sedona_deserialize_geom(buf, buf_size, handle, &geom);
  PyBuffer_Release(&view);
  if (err != SEDONA_SUCCESS) {
    const char *errmsg = sedona_get_error_message(err);
    PyErr_SetString(PyExc_ValueError, errmsg);
    return NULL;
  }

  *out_handle = handle;
  return geom;
}

/* serialize/deserialize functions for Shapely 2.x */

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

  return do_serialize(geos_geom);
}

static PyObject *deserialize(PyObject *self, PyObject *args) {
  GEOSContextHandle_t handle = NULL;
  GEOSGeometry *geom = do_deserialize(args, &handle);
  if (geom == NULL) {
    return NULL;
  }
  PyObject *pygeom = PyGEOS_CreateGeometry(geom, handle);
  return pygeom;
}

/* serialize/deserialize functions for Shapely 1.x */

static PyObject *serialize_1(PyObject *self, PyObject *args) {
  GEOSGeometry *geos_geom = NULL;
  if (!PyArg_ParseTuple(args, "l", &geos_geom)) {
    return NULL;
  }
  return do_serialize(geos_geom);
}

static PyObject *deserialize_1(PyObject *self, PyObject *args) {
  GEOSContextHandle_t handle = NULL;
  GEOSGeometry *geom = do_deserialize(args, &handle);
  if (geom == NULL) {
    return NULL;
  }

  /* These functions would be called by Shapely using ctypes when constructing
   * a Shapely BaseGeometry object from GEOSGeometry pointer. We call them here
   * to get rid of the extra overhead introduced by ctypes. */
  int geom_type_id = pf_GEOSGeomTypeId_r(handle, geom);
  char has_z = pf_GEOSHasZ_r(handle, geom);
  return Py_BuildValue("(lib)", geom, geom_type_id, has_z);
}

/* Module definition for Shapely 2.x */

static PyMethodDef geomserde_methods_shapely_2[] = {
    {"load_libgeos_c", load_libgeos_c, METH_VARARGS, "Load libgeos_c."},
    {"serialize", serialize, METH_VARARGS,
     "Serialize geometry object as bytearray."},
    {"deserialize", deserialize, METH_VARARGS,
     "Deserialize bytes-like object to geometry object."},
    {NULL, NULL, 0, NULL}, /* Sentinel */
};

static struct PyModuleDef geomserde_module_shapely_2 = {
    PyModuleDef_HEAD_INIT, "geomserde_speedup", module_doc, 0,
    geomserde_methods_shapely_2};

/* Module definition for Shapely 1.x */

static PyMethodDef geomserde_methods_shapely_1[] = {
    {"load_libgeos_c", load_libgeos_c, METH_VARARGS, "Load libgeos_c."},
    {"serialize_1", serialize_1, METH_VARARGS,
     "Serialize geometry object as bytearray."},
    {"deserialize_1", deserialize_1, METH_VARARGS,
     "Deserialize bytes-like object to geometry object."},
    {NULL, NULL, 0, NULL}, /* Sentinel */
};

static struct PyModuleDef geomserde_module_shapely_1 = {
    PyModuleDef_HEAD_INIT, "geomserde_speedup", module_doc, 0,
    geomserde_methods_shapely_1};

PyMODINIT_FUNC PyInit_geomserde_speedup(void) {
  if (import_shapely_c_api() != 0) {
    /* As long as the capsule provided by Shapely 2.0 cannot be loaded, we
     * assume that we're working with Shapely 1.0 */
    PyErr_Clear();
    return PyModuleDef_Init(&geomserde_module_shapely_1);
  }

  return PyModuleDef_Init(&geomserde_module_shapely_2);
}
