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

import pytest

from shapely.geometry.base import BaseGeometry
from sedona.utils import geometry_serde

from shapely.geometry import Point
from shapely.geometry import LineString
from shapely.geometry import Polygon
from shapely.geometry import MultiPoint
from shapely.geometry import MultiLineString
from shapely.geometry import MultiPolygon
from shapely.geometry import GeometryCollection
from shapely.wkt import loads as wkt_loads


class TestGeometrySerde:
    @pytest.mark.parametrize("wkt", [
        # empty geometries
        'POINT EMPTY',
        'LINESTRING EMPTY',
        'POLYGON EMPTY',
        'MULTIPOINT EMPTY',
        'MULTILINESTRING EMPTY',
        'MULTIPOLYGON EMPTY',
        'GEOMETRYCOLLECTION EMPTY',
        # non-empty geometries
        'POINT (10 20)',
        'POINT (10 20 30)',
        'LINESTRING (10 20, 30 40)',
        'LINESTRING (10 20 30, 40 50 60)',
        'POLYGON ((10 10, 20 20, 20 10, 10 10))',
        'POLYGON ((10 10 10, 20 20 10, 20 10 10, 10 10 10))',
        'POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))',
        # non-empty multi geometries
        'MULTIPOINT ((10 20), (30 40))',
        'MULTIPOINT ((10 20 30), (40 50 60))',
        'MULTILINESTRING ((10 20, 30 40), (50 60, 70 80))',
        'MULTILINESTRING ((10 20 30, 40 50 60), (70 80 90, 100 110 120))',
        'MULTIPOLYGON (((10 10, 20 20, 20 10, 10 10)), ((-10 -10, -20 -20, -20 -10, -10 -10)))',
        'MULTIPOLYGON (((10 10, 20 20, 20 10, 10 10)), ((0 0, 0 10, 10 10, 10 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1)))',
        'GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (10 20, 30 40))',
        'GEOMETRYCOLLECTION (POINT (10 20 30), LINESTRING (10 20 30, 40 50 60))',
        'GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (10 20, 30 40), POLYGON ((10 10, 20 20, 20 10, 10 10)))',
        # nested geometry collection
        'GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (10 20, 30 40)))',
        'GEOMETRYCOLLECTION (POINT (1 2), GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (10 20, 30 40)))',
        # multi geometries containing empty geometries
        'MULTIPOINT (EMPTY, (10 20))',
        'MULTIPOINT (EMPTY, EMPTY)',
        'MULTILINESTRING (EMPTY, (10 20, 30 40))',
        'MULTILINESTRING (EMPTY, EMPTY)',
        'MULTIPOLYGON (EMPTY, ((10 10, 20 20, 20 10, 10 10)))',
        'MULTIPOLYGON (EMPTY, EMPTY)',
        'GEOMETRYCOLLECTION (POINT (10 20), POINT EMPTY, LINESTRING (10 20, 30 40))',
        'GEOMETRYCOLLECTION (MULTIPOINT EMPTY, MULTILINESTRING EMPTY, MULTIPOLYGON EMPTY, GEOMETRYCOLLECTION EMPTY)',
    ])
    def test_serde(self, wkt):
        geom = wkt_loads(wkt)
        buffer = geometry_serde.serialize(geom)
        geom2, offset = geometry_serde.deserialize(buffer)
        assert geom.equals_exact(geom2, 1e-6)

    def test_point(self):
        points = [
            wkt_loads("POINT EMPTY"),
            Point(10, 20),
            Point(10, 20, 30)
        ]
        self._test_serde_roundtrip(points)

    def test_linestring(self):
        linestrings = [
            wkt_loads("LINESTRING EMPTY"),
            LineString([(10, 20), (30, 40)]),
            LineString([(10, 20), (30, 40), (50, 60)]),
            LineString([(10, 20, 30), (30, 40, 50), (50, 60, 70)]),
        ]
        self._test_serde_roundtrip(linestrings)

    def test_multi_point(self):
        multi_points = [
            wkt_loads("MULTIPOINT EMPTY"),
            MultiPoint([(10, 20)]),
            MultiPoint([(10, 20), (30, 40)]),
            MultiPoint([(10, 20), (30, 40), (50, 60)]),
            MultiPoint([(10, 20, 30), (30, 40, 50), (50, 60, 70)]),
        ]
        self._test_serde_roundtrip(multi_points)

    def test_multi_linestring(self):
        multi_linestrings = [
            wkt_loads("MULTILINESTRING EMPTY"),
            MultiLineString([[(10, 20), (30, 40)]]),
            MultiLineString([[(10, 20), (30, 40)], [(50, 60), (70, 80)]]),
            MultiLineString([[(10, 20, 30), (30, 40, 50)], [(50, 60, 70), (70, 80, 90)]]),
        ]
        self._test_serde_roundtrip(multi_linestrings)

    def test_polygon(self):
        ext = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
        int0 = [(1, 1), (1, 1.5), (1.5, 1.5), (1.5, 1), (1, 1)]
        int1 = [(2, 2), (2, 2.5), (2.5, 2.5), (2.5, 2), (2, 2)]
        polygons = [
            wkt_loads("POLYGON EMPTY"),
            Polygon(ext),
            Polygon(ext, [int0]),
            Polygon(ext, [int0, int1]),
        ]
        self._test_serde_roundtrip(polygons)

    def test_multi_polygon(self):
        ext = [(0, 0), (100, 0), (100, 100), (0, 100), (0, 0)]
        int0 = [(10, 10), (10, 15), (15, 15), (15, 10), (10, 10)]
        int1 = [(2, 2), (2, 2.5), (2.5, 2.5), (2.5, 2), (2, 2)]
        multi_polygons = [
            wkt_loads("MULTIPOLYGON EMPTY"),
            MultiPolygon([Polygon(ext)]),
            MultiPolygon([Polygon(ext), Polygon(ext, [int0])]),
            MultiPolygon([Polygon(ext), Polygon(ext, [int0, int1])]),
            MultiPolygon([Polygon(ext, [int1]), Polygon(ext), Polygon(ext, [int0, int1])]),
        ]
        self._test_serde_roundtrip(multi_polygons)

    def test_geometry_collection(self):
        geometry_collections = [
            wkt_loads("GEOMETRYCOLLECTION EMPTY"),
            GeometryCollection([Point(10, 20), LineString([(10, 20), (30, 40)]), Point(30, 40)]),
            GeometryCollection([
                MultiPoint([(10, 20), (30, 40)]),
                MultiLineString([[(10, 20), (30, 40)], [(50, 60), (70, 80)]]),
                MultiPolygon([
                    Polygon(
                        [(0, 0), (100, 0), (100, 100), (0, 100), (0, 0)],
                        [[(10, 10), (10, 15), (15, 15), (15, 10), (10, 10)]])
                ]),
                Point(100, 200)
            ]),
            GeometryCollection([
                GeometryCollection([Point(10, 20), LineString([(10, 20), (30, 40)]), Point(30, 40)]),
                GeometryCollection([
                    MultiPoint([(10, 20), (30, 40)]),
                    MultiLineString([[(10, 20), (30, 40)], [(50, 60), (70, 80)]]),
                    Point(10, 20)
                ])
            ])
        ]
        self._test_serde_roundtrip(geometry_collections)

    @staticmethod
    def _test_serde_roundtrip(geoms):
        for geom in geoms:
            geom_actual = TestGeometrySerde.serde_roundtrip(geom)
            assert geom_actual.equals_exact(geom, 1e-6)
            # GEOSGeom_createEmptyLineString in libgeos creates LineString with
            # Z dimension, This bug has been fixed by
            # https://github.com/libgeos/geos/pull/745
            geom_actual_wkt = geom_actual.wkt.replace('LINESTRING Z EMPTY', 'LINESTRING EMPTY')
            assert geom.wkt == geom_actual_wkt

    @staticmethod
    def serde_roundtrip(geom: BaseGeometry) -> BaseGeometry:
        buffer = geometry_serde.serialize(geom)
        geom2, offset = geometry_serde.deserialize(buffer)
        return geom2
