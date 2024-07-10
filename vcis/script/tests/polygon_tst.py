from shapely.geometry import Point, LineString
import shapely.geometry
from shapely.ops import unary_union
import pandas as pd
import numpy as np
import shapely
from math import atan2, pi,cos,sin,sqrt
from vcis.utils.utils import CDR_Properties, CDR_Utils
import folium
from shapely.geometry import Polygon
import matplotlib.pyplot as plt
properties = CDR_Properties()
utils = CDR_Utils()

# Assuming df is loaded from a CSV file as shown in your snippet
df = pd.read_csv(properties.passed_filepath_excel + 'df_main.csv')
df = df.sample(50)
# Ensure latitude and longitude columns are floats
df['latitude_grid'] = df['latitude_grid'].astype(float)
df['longitude_grid'] = df['longitude_grid'].astype(float)

df = df.sort_values(by='usage_timeframe')

neighbor_lists = []
current_list = []
step_lat ,step_lon = utils.get_step_size(distance=100)
def is_neighbor(grid1, grid2, step_lat=0.1, step_lon=0.1):
    lat1, lon1 = map(float, grid1.split(','))
    lat2, lon2 = map(float, grid2.split(','))
    return (abs(lat1 - lat2) <= step_lat) and (abs(lon1 - lon2) <= step_lon)

for index, row in df.iterrows():
    if index < len(df) - 1:
        next_row = df.iloc[index + 1]
        if is_neighbor(row['grid'], next_row['grid'],step_lat,step_lon):
            current_list.append(row['grid'])
        else:
            if len(current_list) !=0:
                neighbor_lists.append(current_list)
                current_list = []
                current_list.append(next_row['grid'])
    else:
        break


print(neighbor_lists)
# Get the dynamic radius
