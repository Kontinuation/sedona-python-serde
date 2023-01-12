from sedona.utils import geometry_serde
from shapely.geometry import Point
from shapely.geometry import Polygon


p = geometry_serde.deserialize(b'11122222334')
print(p, type(p))

b = bytearray(b'1234567890')

p = geometry_serde.deserialize(b)
print(p, type(p))

p = geometry_serde.deserialize(b[2::2])
print(p, type(p))

# v = memoryview(b)
# print(v[2::2])
# p = geomserde.deserialize(v[2::2])
# print(p, type(p))

print(isinstance(p, Point))
b = geometry_serde.serialize(p)
print(b, len(b))

# b = geometry_serde.serialize(Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]))
# print(b)
