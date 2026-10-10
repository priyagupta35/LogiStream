import math
import unittest

def calculate_haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance in km used to verify Redis GEO proximity matching."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

class TestDispatchLogic(unittest.TestCase):
    def test_nearest_driver_distance_calculation(self):
        pickup = (22.5726, 88.3639)
        driver_near = (22.5780, 88.3680)
        driver_far = (22.7500, 88.5000)

        dist_near = calculate_haversine_km(pickup[0], pickup[1], driver_near[0], driver_near[1])
        dist_far = calculate_haversine_km(pickup[0], pickup[1], driver_far[0], driver_far[1])

        self.assertLess(dist_near, 1.0)  # Under 1km
        self.assertGreater(dist_far, 20.0)  # Over 20km
        self.assertLess(dist_near, dist_far)

if __name__ == "__main__":
    unittest.main()
