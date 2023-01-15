from sedona.utils import geometry_serde
from shapely.geometry import Point
from shapely.geometry import Polygon


p = Point(10, 20)
b = geometry_serde.serialize(p)
print(b, len(b))

p = geometry_serde.deserialize(b)
print(p, isinstance(p, Point))

b = geometry_serde.serialize(Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]))
print(b)
