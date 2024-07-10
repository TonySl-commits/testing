import math
import numpy as np
import geopy.distance

def get_step_size(distance:int=200):
    one_degree_lat = 110.574
    one_degree_lat_radians = math.radians(lat1)
    one_degree_long = 111.320*math.cos(one_degree_lat_radians)
    step_lon = (distance / (one_degree_long*1000))
    step_lat = (distance / (one_degree_lat*1000))
    return step_lat,step_lon 

def binning_lat(data,step):
    step = step
    to_bin = lambda x: np.floor(x / step) * step
    data.loc[:, "latitude_grid"] = data['location_latitude'].apply(to_bin)
    return data

def binning_lon(data,step):
    step = step
    to_bin = lambda x: np.floor(x / step) * step
    data.loc[:, "longitude_grid"] = data['location_longitude'].apply(to_bin)
    return data

def binning(data,distance:int=200):
    step_lat,step_lon = get_step_size(distance)
    data = binning_lat(data,step_lat)
    data = binning_lon(data,step_lon)
    return data

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))

    r = 6371.0

    return c * r 

distance_in_meters = 100
number = 33

# one_degree_lat = 110.574
# one_degree_lat_radians = math.radians(lat1)
# one_degree_long = 111.320*math.cos(one_degree_lat_radians)

# distance_in_degree_long = (distance_in_meters / (one_degree_long*1000))
# distance_in_degree_lat = (distance_in_meters / (one_degree_lat*1000))

# print(distance_in_degree_long)
# print(distance_in_degree_lat)

lat1 = 0
lon1 = 0

lat2 = 0 - 0.1
lon2 = 0
coords_1 = (lat1, lon1)
coords_2 = (lat2, lon2)

distance = geopy.distance.geodesic(coords_1, coords_2).m
print(distance)
#11091.070142283961
#11057.427694902271


