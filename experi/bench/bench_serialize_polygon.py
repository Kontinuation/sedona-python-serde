from shapely.geometry import Point, Polygon
import shapely.wkb
from sedona.utils import geometry_serde


NUM_ITERS = 10000
polygon = Point(0, 0).buffer(1, 100)


def serialize_polygon_wkb():
    for k in range(0, NUM_ITERS):
        shapely.wkb.dumps(polygon)


def serialize_polygon_geomserde():
    for k in range(0, NUM_ITERS):
        geometry_serde.serialize(polygon)


__benchmarks__ = [
    (serialize_polygon_wkb, serialize_polygon_geomserde, "Serialize Polygons")
]
