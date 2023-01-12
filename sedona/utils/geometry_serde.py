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

from typing import Optional
from warnings import warn

import shapely
from shapely.geometry.base import BaseGeometry


# Use geomserde_speedup when available, otherwise fallback to general pure
# python implementation.
try:
    from . import geomserde_speedup

    if shapely.__version__.startswith('2.'):
        # We load geos_c library by loading shapely.lib
        import shapely.lib
        geomserde_speedup.load_libgeos_c(shapely.lib.__file__)
        from .geomserde_speedup import serialize

        def deserialize(buf: bytearray) -> Optional[BaseGeometry]:
            if buf is None:
                return None
            return geomserde_speedup.deserialize(buf)

    elif shapely.__version__.startswith('1.'):
        # Shapely 1.x uses ctypes.CDLL to load geos_c library. We can obtain the
        # handle of geos_c library from `shapely.geos._lgeos._handle`
        import shapely.geos
        import shapely.geometry.base
        from shapely.geometry import (
            Point,
            LineString,
            LinearRing,
            Polygon,
            MultiPoint,
            MultiLineString,
            MultiPolygon,
            GeometryCollection
        )
        lgeos_handle = shapely.geos._lgeos._handle
        geomserde_speedup.load_libgeos_c(lgeos_handle)

        GEOMETRY_CLASSES = [
            Point,
            LineString,
            LinearRing,
            Polygon,
            MultiPoint,
            MultiLineString,
            MultiPolygon,
            GeometryCollection,
        ]

        def serialize(geom: BaseGeometry) -> Optional[bytearray]:
            if geom is None:
                return None
            return geomserde_speedup.serialize_1(geom._geom)

        def deserialize(buf: bytearray) -> Optional[BaseGeometry]:
            if buf is None:
                return None
            g, geom_type_id, has_z = geomserde_speedup.deserialize_1(buf)

            # The following code is mostly taken from the geom_factory function in
            # shapely/geometry/base.py, with a few tweaks to eliminate invocations
            # to GEOS functions.
            if not g:
                raise ValueError("No Shapely geometry can be created from null value")
            ob = BaseGeometry()
            geom_type = shapely.geometry.base.GEOMETRY_TYPES[geom_type_id]
            ob.__class__ = GEOMETRY_CLASSES[geom_type_id]
            ob._set_geom(g)
            ob.__p__ = None
            if has_z != 0:
                ob._ndim = 3
            else:
                ob._ndim = 2
            ob._is_empty = False
            return ob

    else:
        # fallback to our general pure python implementation
        from .geomserde_general import serialize, deserialize

except Exception as e:
    warn(f'Cannot load geomserde_speedup, fallback to general python implementation. Reason: {e}')
    from .geomserde_general import serialize, deserialize
