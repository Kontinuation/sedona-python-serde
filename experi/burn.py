from sedona.utils import geometry_serde
from shapely.geometry import Point
from shapely.geometry import Polygon

p = Point(10, 20)
buf = geometry_serde.serialize(p)

while True:
    geometry_serde.serialize(p)
    geometry_serde.deserialize(buf)
