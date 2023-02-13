from shapely.geometry import LineString, Point, Polygon, MultiPoint, MultiLineString, MultiPolygon
import shapely.wkb
from sedona.utils import geometry_serde


short_line_iterations = 20_000
short_line = LineString([(10.0, 10.0), (20.0, 20.0)])

long_line_iterations = 10_000
long_line = LineString([(float(n), float(n)) for n in range(1000)])

point_iterations = 50_000
point = Point(12.3, 45.6)

small_polygon_iterations = 20_000
small_polygon = Polygon([(10.0, 10.0), (20.0, 10.0), (20.0, 20.0), (10.0, 20.0), (10.0, 10.0)])

large_polygon_iterations = 10_000
large_polygon = Polygon(
    [(0.0, float(n * 10)) for n in range(100)]
    + [(float(n * 10), 990.0) for n in range(100)]
    + [(990.0, float(n * 10)) for n in reversed(range(100))]
    + [(float(n * 10), 0.0) for n in reversed(range(100))]
)

small_multipoint_iterations = 10_000
small_multipoint = MultiPoint([(n, n) for n in range(3)])

large_multipoint_iterations = 1_000
large_multipoint = MultiPoint([(n, n) for n in range(100)])

small_multilinestring_iterations = 10_000
small_multilinestring = MultiLineString([[(10.0, 10.0), (20.0, 20.0)] for _ in range(3)])

large_multilinestring_iterations = 1_000
large_multilinestring = MultiLineString([[(10.0, 10.0), (20.0, 20.0)] for _ in range(100)])

small_multipolygon_iterations = 10_000
small_multipolygon = MultiPolygon([small_polygon for _ in range(3)])

large_multipolygon_iterations = 1_000
large_multipolygon = MultiPolygon([small_polygon for _ in range(100)])


def bench_serialize(geom, iterations, name):

    def serialize_wkb():
        for k in range(iterations):
            shapely.wkb.dumps(geom)

    def serialize_sedona():
        for k in range(iterations):
            geometry_serde.serialize(geom)

    return (serialize_wkb, serialize_sedona, "serialize - " + name)


def bench_deserialize(geom, iterations, name):

    geom_wkb = shapely.wkb.dumps(geom)
    geom_sedona = geometry_serde.serialize(geom)

    def deserialize_wkb():
        for k in range(iterations):
            shapely.wkb.loads(geom_wkb)

    def deserialize_sedona():
        for k in range(iterations):
            geometry_serde.deserialize(geom_sedona)

    return (deserialize_wkb, deserialize_sedona, "deserialize - " + name)


__benchmarks__ = [
    bench_serialize(short_line, short_line_iterations, "short line"),
    bench_serialize(long_line, long_line_iterations, "long line"),
    bench_serialize(point, point_iterations, "point"),
    bench_serialize(small_polygon, small_polygon_iterations, "small polygon"),
    bench_serialize(large_polygon, large_polygon_iterations, "large polygon"),
    bench_serialize(small_multipoint, small_multipoint_iterations, "small multipoint"),
    bench_serialize(large_multipoint, large_multipoint_iterations, "large multipoint"),
    bench_serialize(small_multilinestring, small_multilinestring_iterations, "small multilinestring"),
    bench_serialize(large_multilinestring, large_multilinestring_iterations, "large multilinestring"),
    bench_serialize(small_multipolygon, small_multipolygon_iterations, "small multipolygon"),
    bench_serialize(large_multipolygon, large_multipolygon_iterations, "large multipolygon"),

    bench_deserialize(short_line, short_line_iterations, "short line"),
    bench_deserialize(long_line, long_line_iterations, "long line"),
    bench_deserialize(point, point_iterations, "point"),
    bench_deserialize(small_polygon, small_polygon_iterations, "small polygon"),
    bench_deserialize(large_polygon, large_polygon_iterations, "large polygon"),
    bench_deserialize(small_multipoint, small_multipoint_iterations, "small multipoint"),
    bench_deserialize(large_multipoint, large_multipoint_iterations, "large multipoint"),
    bench_deserialize(small_multilinestring, small_multilinestring_iterations, "small multilinestring"),
    bench_deserialize(large_multilinestring, large_multilinestring_iterations, "large multilinestring"),
    bench_deserialize(small_multipolygon, small_multipolygon_iterations, "small multipolygon"),
    bench_deserialize(large_multipolygon, large_multipolygon_iterations, "large multipolygon"),
]
