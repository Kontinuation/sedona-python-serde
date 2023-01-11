from shapely.geometry import Point
import shapely.wkb
from sedona.utils import geometry_serde


NUM_ITERS = 10000


def serialize_point_wkb():
    p = Point(10, 20)
    for k in range(0, NUM_ITERS):
        shapely.wkb.dumps(p)


def serialize_point_geomserde():
    p = Point(10, 20)
    for k in range(0, NUM_ITERS):
        geometry_serde.serialize(p)


__benchmarks__ = [
    (serialize_point_wkb, serialize_point_geomserde, "Serialize Points")
]
