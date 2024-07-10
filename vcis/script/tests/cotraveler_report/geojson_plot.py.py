import pandas as pd
from vcis.utils.utils import CDR_Utils,CDR_Properties
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
import folium
from folium.plugins import TimestampedGeoJson
from folium.plugins import HeatMap

import folium
import math
utils = CDR_Utils()
properties = CDR_Properties()
df_history = pd.read_csv(properties.passed_filepath_excel + 'df_history.csv')
df_common = pd.read_csv(properties.passed_filepath_excel +'df_common.csv')

pd.set_option('display.max_columns', None)
df_common = utils.convert_datetime_to_ms(df_common)
df_common = df_common.drop_duplicates(subset=['device_id','usage_timeframe'])
df_common = df_common[['device_id', 'location_latitude', 'location_longitude','usage_timeframe','grid']]
df_history = df_history[['device_id', 'location_latitude', 'location_longitude','usage_timeframe','grid']]
df_common['usage_timeframe'] = pd.to_datetime(df_common['usage_timeframe'], unit='ms')
x=0
for device_id in df_history['device_id'].unique():
    df_device = df_history[df_history['device_id'] == device_id]
    df_device_common = df_common[df_common['device_id'] ==device_id]
    if x==32:
        break
    x=x+1

df_main = pd.read_csv(properties.passed_filepath_excel + 'df_device.csv')

fig = go.Figure()

grid_counts = df_device_common.groupby(['grid']).size().reset_index(name='count')
grid_counts['latitude_grid'],grid_counts['longitude_grid']= grid_counts['grid'].str.split(',').str

df_heatmap = df_device_common.groupby(['location_latitude','location_longitude']).size().reset_index(name='count')

print(grid_counts.shape,df_device_common.shape)
# Create a map centered around the mean latitude and longitude of the main device
m = folium.Map(location=[df_main['location_latitude'].mean(), df_main['location_longitude'].mean()], zoom_start=6)

# Prepare the GeoJSON data
geojson = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row['location_longitude'], row['location_latitude']]
            },
            "properties": {
                "time": row['usage_timeframe'].strftime('%Y-%m-%dT%H:%M:%SZ'), # ISO 8601 format
                "popupContent": f"Device ID: {row['device_id']}<br>Location: {row['location_latitude']}, {row['location_longitude']}<br>Usage Timeframe: {row['usage_timeframe']}"
            }
        } for index, row in df_common.iterrows()
    ]
}

# Add the TimestampedGeoJson layer
TimestampedGeoJson(
    geojson,
    period='PT10m', 
    add_last_point=True,
    auto_play=False,
    loop=False,
    max_speed=1,
    loop_button=True,
    date_options='YYYY-MM-DD HH:mm:ss',
    time_slider_drag_update=True
).add_to(m)

# Display the map
m.show_in_browser()