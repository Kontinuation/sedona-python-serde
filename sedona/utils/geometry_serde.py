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

import shapely

from . import geomserde


if shapely.__version__.startswith('2.'):
    # We load geos_c library by loading shapely.lib
    import shapely.lib
    geomserde.load_libgeos_c(shapely.lib.__file__)
    from geomserde import serialize
    from geomserde import deserialize

elif shapely.__version__.startswith('1.'):
    # Shapely 1.x uses ctypes.CDLL to load geos_c library. We can obtain the
    # handle of geos_c library from `shapely.geos._lgeos._handle`
    import shapely.geos
    lgeos_handle = shapely.geos._lgeos._handle
    geomserde.load_libgeos_c(lgeos_handle)

    # TODO: wrap geomserde.serialize and geomserde.deserialize
else:
    # fallback to our general pure python implementation
    # TODO: implement this
    pass
