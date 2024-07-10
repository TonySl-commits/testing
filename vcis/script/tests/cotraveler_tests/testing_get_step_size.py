import numpy as np
import pandas as pd
import math
import plotly.graph_objects as go
import geopy.distance

def get_step_size(latitude,distance:int=200):
    one_degree_lat = 110.574
    one_degree_lat_radians = math.radians(latitude)
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
    latitude = data['location_latitude'].head(1).values[0]
    step_lat,step_lon = get_step_size(latitude,distance)
    
    data = binning_lat(data,step_lat)
    data = binning_lon(data,step_lon)
    return data

start = np.array([33.44, 33.44])
end = np.array([33.45,33.45])

# Calculate the number of points needed
num_points = 1000

# Generate the points along the line
points = np.linspace(start, end, num_points)

# Separate the points into latitude and longitude
location_latitude = points[:, 0]
location_longitude = points[:, 1]


df = pd.DataFrame()

df['location_latitude'] = location_latitude
df['location_longitude'] = location_longitude


df = binning(df,distance=100)


df = df.groupby(['latitude_grid','longitude_grid']).size().reset_index(name='counts')
print(df)


lat1 = 33.439145
lon1 = 33.439743
lat2 = 33.440049
lon2 = 33.439743
coords_1 = (lat1, lon1)
coords_2 = (lat2, lon2)

distance = geopy.distance.geodesic(coords_1, coords_2).m
print(distance)
