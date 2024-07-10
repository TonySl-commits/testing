import math
import numpy as np


# Function to calculate azimuth angle between two points
def calculate_azimuth_angle(origin, destination):
    lat1 = math.radians(origin[0])
    lon1 = math.radians(origin[1])
    lat2 = math.radians(destination[0])
    lon2 = math.radians(destination[1])

    dlon = lon2 - lon1
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1) * math.cos(lat2) * math.cos(dlon))

    calc_azimuth = math.degrees(math.atan2(x, y))
    calc_azimuth = (calc_azimuth + 360) % 360
    return calc_azimuth

def haversine(lon1, lat1, lon2, lat2):
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))

    # Radius of earth in kilometers
    r = 6371.0

    # Return distance in kilometers
    return c * r 

# Function to check if a point is within the sector
def is_within_sector(point, center, left_bound, right_bound, max_distance):
    angle = calculate_azimuth_angle(center, point)
    # distance = geopy.distance.great_circle(center, point).meters
    distance = haversine(center[0], center[1], point[0], point[1])
    return left_bound <= angle <= right_bound and distance <= max_distance

def possible_points(nodes, coordinates, azymuth, sector_width = 120, distance= 500):

        # Calculate the bounds of the sector
        left_bound_angle = azymuth - (sector_width / 2)
        right_bound_angle = azymuth + (sector_width / 2)

        # Filter nodes within the BTS sector
        nodes_in_sector = nodes[nodes.apply(lambda x: is_within_sector((x['y'], x['x']), coordinates, left_bound_angle, right_bound_angle, distance), axis=1)]
        min_distance = float('inf')
        closest_point = None
        
        for _, row in nodes_in_sector.iterrows():
            point = (row['y'], row['x'])
            dist = haversine(coordinates[0],coordinates[1], point[0],point[1])
            if dist < min_distance:
                min_distance = dist
                closest_point = point

        # # If Using G function Method
                
        # nodes_in_sector['distance'] = nodes_in_sector.apply(lambda row: geopy.distance.distance(coordinates, (row['y'], row['x'])).meters, axis=1)
        # nodes_in_sector = nodes_in_sector.reset_index(drop=True)
        # min_distance_idx = nodes_in_sector['distance'].idxmin()
        # closest_point = tuple(nodes_in_sector.loc[min_distance_idx, ['y', 'x']])

        return nodes_in_sector, closest_point
