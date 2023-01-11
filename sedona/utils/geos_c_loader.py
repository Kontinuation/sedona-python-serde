#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import sys
import os
import glob
from ctypes import CDLL
from ctypes.util import find_library

import shapley

from . import geomserde


LOG = logging.getLogger(__name__)


def load_geos_c_library():
    """Load geos_c library for geomserde."""
    if sys.platform.startswith('linux'):
        _load_geos_c_library_linux()
    elif sys.platform == 'darwin':
        _load_geos_c_library_darwin()
    elif sys.platform == 'win32':
        _load_geos_c_library_win32()
    else:
        _load_library('geos_c', fallbacks=['libgeos_c.so.1', 'libgeos_c.so'])


def _load_library(libname, fallbacks=None, mode=DEFAULT_MODE):
    pass


def _load_geos_c_library_linux():
    # Test to see if we have a wheel repaired by auditwheel which contains its
    # own libgeos_c. Note: auditwheel 3.1 changed the location of libs.
    geos_whl_so = glob.glob(
        os.path.abspath(os.path.join(os.path.dirname(shapely.__file__), ".libs/libgeos*.so*"))
    ) or glob.glob(
        os.path.abspath(
            os.path.join(
                os.path.dirname(shapely.__file__), "..", "Shapely.libs", "libgeos*.so*"
            )
        )
    ) or glob.glob(
        os.path.abspath(
            os.path.join(
                os.path.dirname(shapely.__file__), "..", "shapely.libs", "libgeos*.so*"
            )
        )
    )

    if len(geos_whl_so) > 0:
        geomserde.load_libgeos_c(geos_whl_so[0])
    elif hasattr(sys, 'frozen'):
        geos_pyinstaller_so = glob.glob(os.path.join(sys.prefix, 'libgeos_c-*.so.*'))
        geomserde.load_libgeos_c(geos_pyinstaller_so[0])
    elif exists_conda_env():
        # conda package.
        _load_library(os.path.join(sys.prefix, 'lib', 'libgeos_c.so'))
    else:
        alt_paths = [
            'libgeos_c.so.1',
            'libgeos_c.so',
        ]
        _load_library(find_library('geos_c'), fallbacks=alt_paths)


def _find_geos_c_library_darwin():
    # Test to see if we have a delocated wheel with a GEOS dylib.
    dylib_path = os.path.abspath(
        os.path.join(os.path.dirname(shapely.__file__), ".dylibs/*.dylib")
    )
    LOG.debug("Formed path for globbing: dylib_path=%r", dylib_path)

    geos_whl_dylib = glob.glob(dylib_path)
    LOG.debug("Globbed: geos_whl_dylib=%r", geos_whl_dylib)

    if len(geos_whl_dylib) > 0:
        handle = CDLL(None)
        if hasattr(handle, "initGEOS_r"):
            LOG.debug("GEOS already loaded")
            return None
        else:
            geos_whl_dylib = sorted(geos_whl_dylib)
            for lib in geos_whl_dylib:
                if 'geos_c' in lib:
                    return lib
            return geos_whl_dylib[0]
    elif hasattr(sys, "frozen"):
        try:
            # .app file from py2app
            alt_paths = [
                os.path.join(
                    os.environ["RESOURCEPATH"], "..", "Frameworks", "libgeos_c.dylib"
                )
            ]
        except KeyError:
            alt_paths = [
                # binary from pyinstaller
                os.path.join(sys.executable, "libgeos_c.dylib"),
                # .app from cx_Freeze
                os.path.join(os.path.dirname(sys.executable), "libgeos_c.1.dylib"),
            ]
            if hasattr(sys, "_MEIPASS"):
                alt_paths.append(os.path.join(sys._MEIPASS, "libgeos_c.1.dylib"))
        libgeos = find_library('geos_c')
        if libgeos:
            return libgeos
        for libgeos in alt_paths:
            if os.path.exists(libgeos):
                return libgeos
        return None
    elif exists_conda_env():
        # conda package.
        return os.path.join(sys.prefix, 'lib', 'libgeos_c.dylib')
    else:
        alt_paths = [
            # The Framework build from Kyng Chaos
            "/Library/Frameworks/GEOS.framework/Versions/Current/GEOS",
            # macports
            "/opt/local/lib/libgeos_c.dylib",
            # homebrew Intel
            "/usr/local/lib/libgeos_c.dylib",
            # homebrew Apple Silicon
            "/opt/homebrew/lib/libgeos_c.dylib",
        ]
        libgeos = find_library('geos_c')
        if libgeos:
            return libgeos
        for libgeos in alt_paths:
            if os.path.exists(libgeos):
                return libgeos
        return None


def _find_geos_c_library_win32():
    _conda_dll_path = os.path.join(sys.prefix, 'Library', 'bin', 'geos_c.dll')

    if exists_conda_env() and os.path.exists(_conda_dll_path):
        return _conda_dll_path
    else:
        geos_whl_dll = glob.glob(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(shapely.__file__), "..", "Shapely.libs", "geos*.dll"
                )
            )
        )

        if len(geos_whl_dll) > 0:
            geos_whl_dll = sorted(geos_whl_dll)
            return geos_whl_dll[-1]
        else:
            try:
                egg_dlls = os.path.abspath(
                    os.path.join(os.path.dirname(shapely.__file__), "DLLs")
                )
                if hasattr(sys, "_MEIPASS"):
                    wininst_dlls = sys._MEIPASS
                elif hasattr(sys, "frozen"):
                    wininst_dlls = os.path.normpath(
                        os.path.abspath(sys.executable + "../../DLLS")
                    )
                else:
                    wininst_dlls = os.path.abspath(os.shapely.__file__ + "../../../DLLs")
                original_path = os.environ["PATH"]
                os.environ["PATH"] = "%s;%s;%s" % (
                    egg_dlls,
                    wininst_dlls,
                    original_path,
                )
                return find_library("geos_c.dll")
            except (ImportError, WindowsError, OSError):
                raise


def exists_conda_env():
    """Does this module exist in a conda environment?"""
    return os.path.exists(os.path.join(sys.prefix, 'conda-meta'))
