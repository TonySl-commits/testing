import pandas as pd
import numpy as np
import math

from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
import plotly.graph_objects as go

utils = CDR_Utils()
properties = CDR_Properties()

def get_step_size(distance:int=200):
    one_degree = 40075 /360
    inner_distance = distance / math.sqrt(2)
    step_size = (inner_distance / (one_degree*1000))
    return step_size 

step = get_step_size(200)
to_bin = lambda x: np.floor(x / step) * step

# latitude =33.0
# longitude =33.0
lat_list=[]
lon_list=[]
df_common = pd.read_csv('data/excel/df_common_backup.csv')
df_common['lat'],df_common['lon']= df_common['grid'].str.split(',').str

df_common['lat'] = df_common['lat'].astype(float)
df_common['lon'] = df_common['lon'].astype(float)
for i,row in df_common.iterrows():
    latitude = row['lat']
    longitude = row['lon']
    latitude_left = latitude - step
    latitude_right = latitude + step
    longitude_up = longitude - step
    longitude_down = longitude + step
    
    lat_list.append(latitude)
    lon_list.append(longitude)
    
    lat_list.append(latitude_left)
    lon_list.append(longitude)
    
    lat_list.append(latitude_right)
    lon_list.append(longitude)
    
    lat_list.append(latitude)
    lon_list.append(longitude_up)
    
    lat_list.append(latitude)
    lon_list.append(longitude_down)
    
    lat_list.append(latitude_left)
    lon_list.append(longitude_down)

    lat_list.append(latitude_left)
    lon_list.append(longitude_up)
    
    lat_list.append(latitude_right)
    lon_list.append(longitude_down)

    lat_list.append(latitude_right)
    lon_list.append(longitude_up)
    
    latitude = latitude_right
    longitude = longitude

df = pd.DataFrame()
df['lat'] = lat_list
df['lon'] = lon_list

trace_device = go.Scattermapbox(
    lat=df['lat'],
    lon=df['lon'],
    mode='markers',
    marker=dict(
        size=10,
        color='blue',
        opacity=0.7
    ),
    name='Geo_Device'
)
# Create the layout for the map
layout = go.Layout(
    title='Grid Map Locations',
    mapbox=dict(
        style='open-street-map',
        zoom=10
    ),
    showlegend=True
)

fig = go.Figure(data=trace_device, layout=layout)
fig.write_html('Correlation_map.html')

print('################ Device History Map Created ################')