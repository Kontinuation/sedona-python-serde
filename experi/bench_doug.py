from sedona.utils.geometry_serde import serialize, deserialize
from shapely.geometry import LineString, Point, Polygon, MultiPoint, MultiLineString, MultiPolygon
from shapely.wkb import dumps, loads

import time

def run_serialize_trial(geom, number_iterations, name):
    print(f"{name} serialize trial:")

    start_time = time.perf_counter_ns()
    for _ in range(number_iterations):
        dumps(geom)
    shapely_time = time.perf_counter_ns() - start_time

    start_time = time.perf_counter_ns()
    for _ in range(number_iterations):
        serialize(geom)
    sedona_time = time.perf_counter_ns() - start_time

    print(f"\tTotal Time (seconds):")
    print(f"\t\tShapely: {shapely_time / 1e9}\n\t\tSedona: {sedona_time / 1e9}\n\t\tFactor: {(sedona_time - shapely_time) / shapely_time}\n")

def run_deserialize_trial(geom, number_iterations, name):
    print(f"{name} deserialize trial:")

    shapely_serialized_geom = dumps(geom)
    sedona_serialized_geom = serialize(geom)

    start_time = time.perf_counter_ns()
    for _ in range(number_iterations):
        loads(shapely_serialized_geom)
    shapely_time = time.perf_counter_ns() - start_time

    start_time = time.perf_counter_ns()
    for _ in range(number_iterations):
        deserialize(sedona_serialized_geom)
    sedona_time = time.perf_counter_ns() - start_time

    print(f"\tTotal Time (seconds):")
    print(f"\t\tShapely: {shapely_time / 1e9}\n\t\tSedona: {sedona_time / 1e9}\n\t\tFactor: {(sedona_time - shapely_time) / shapely_time}\n")

short_line_iterations = 200_000
short_line = LineString([(10.0, 10.0), (20.0, 20.0)])

long_line_iterations = 100_000
long_line = LineString([(float(n), float(n)) for n in range(1000)])

point_iterations = 500_000
point = Point(12.3, 45.6)

small_polygon_iterations = 200_000
small_polygon = Polygon([(10.0, 10.0), (20.0, 10.0), (20.0, 20.0), (10.0, 20.0), (10.0, 10.0)])

large_polygon_iterations = 100_000
large_polygon = Polygon(
    [(0.0, float(n * 10)) for n in range(100)]
    + [(float(n * 10), 990.0) for n in range(100)]
    + [(990.0, float(n * 10)) for n in reversed(range(100))]
    + [(float(n * 10), 0.0) for n in reversed(range(100))]
)

small_multipoint_iterations = 10_000
small_multipoint = MultiPoint([(n, n) for n in range(3)])

large_multipoint_iterations = 10_000
large_multipoint = MultiPoint([(n, n) for n in range(100)])

small_multilinestring_iterations = 10_000
small_multilinestring = MultiLineString([[(10.0, 10.0), (20.0, 20.0)] for _ in range(3)])

large_multilinestring_iterations = 5_000
large_multilinestring = MultiLineString([[(10.0, 10.0), (20.0, 20.0)] for _ in range(100)])

small_multipolygon_iterations = 10_000
small_multipolygon = MultiPolygon([small_polygon for _ in range(3)])

large_multipolygon_iterations = 5_000
large_multipolygon = MultiPolygon([small_polygon for _ in range(100)])

run_serialize_trial(short_line, short_line_iterations, "short line")
run_serialize_trial(long_line, long_line_iterations, "long line")
run_serialize_trial(point, point_iterations, "point")
run_serialize_trial(small_polygon, small_polygon_iterations, "small polygon")
run_serialize_trial(large_polygon, large_polygon_iterations, "large polygon")
run_serialize_trial(small_multipoint, small_multipoint_iterations, "small multipoint")
run_serialize_trial(large_multipoint, large_multipoint_iterations, "large multipoint")
run_serialize_trial(small_multilinestring, small_multilinestring_iterations, "small multilinestring")
run_serialize_trial(large_multilinestring, large_multilinestring_iterations, "large multilinestring")
run_serialize_trial(small_multipolygon, small_multipolygon_iterations, "small multipolygon")
run_serialize_trial(large_multipolygon, large_multipolygon_iterations, "large multipolygon")

run_deserialize_trial(short_line, short_line_iterations, "short line")
run_deserialize_trial(long_line, long_line_iterations, "long line")
run_deserialize_trial(point, point_iterations, "point")
run_deserialize_trial(small_polygon, small_polygon_iterations, "small polygon")
run_deserialize_trial(large_polygon, large_polygon_iterations, "large polygon")
run_deserialize_trial(small_multipoint, small_multipoint_iterations, "small multipoint")
run_deserialize_trial(large_multipoint, large_multipoint_iterations, "large multipoint")
run_deserialize_trial(small_multilinestring, small_multilinestring_iterations, "small multilinestring")
run_deserialize_trial(large_multilinestring, large_multilinestring_iterations, "large multilinestring")
run_deserialize_trial(small_multipolygon, small_multipolygon_iterations, "small multipolygon")
run_deserialize_trial(large_multipolygon, large_multipolygon_iterations, "large multipolygon")
