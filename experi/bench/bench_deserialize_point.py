from shapely.geometry import Point
import shapely.wkb
from sedona.utils import geometry_serde


NUM_ITERS = 10000


def deserialize_point_wkb():
    p = Point(10, 20)
    buf = shapely.wkb.dumps(p)
    for k in range(0, NUM_ITERS):
        shapely.wkb.loads(buf)


def deserialize_point_geomserde():
    p = Point(10, 20)
    buf = geometry_serde.serialize(p)
    for k in range(0, NUM_ITERS):
        geometry_serde.deserialize(buf)


__benchmarks__ = [
    (deserialize_point_wkb, deserialize_point_geomserde, "Deserialize Points")
]
