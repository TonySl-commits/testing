import pandas as pd
from vcis.utils.utils import CDR_Utils,CDR_Properties
import plotly.graph_objects as go
import plotly.express as px
import numpy as np

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

# Create a map centered around the mean latitude and longitude of the main device
m = folium.Map(location=[df_main['location_latitude'].mean(), df_main['location_longitude'].mean()], zoom_start=3 ,max_zoom= 6)

# Add circles for the main device
for index, row in df_main.iterrows():
    folium.Circle(
        location=[row['location_latitude'], row['location_longitude']],
        radius=2, # Adjust the radius as needed
        color='black'
    ).add_to(m)

# Add circles for the cotraveler device
for index, row in df_device.iterrows():
    folium.Circle(
        location=[row['location_latitude'], row['location_longitude']],
        radius=1, # Adjust the radius as needed
        color='blue'
    ).add_to(m)

# Define the color gradient
color_gradient = {
    0: 'green',
    0.5: 'yellow',
    1: 'red'
}

# Add a heatmap for common hits
heat_data = [[row['location_latitude'], row['location_longitude'], row['count']] for index, row in df_heatmap.iterrows()]
heatmap = HeatMap(heat_data, radius=20, gradient=color_gradient)
heatmap.add_to(m)

# Add a custom legend
legend_html = """
<div style="position: fixed; bottom: 50px; left: 50px; width: 150px; height: 200px; background-color: white; z-index:9999; font-size:14px; border-radius: 5px; box-shadow: 0 0 10px rgba(0,0,0,0.1); padding: 10px;">
    <p>Low</p>
    <div style="width: 100%; height: 100px; background: linear-gradient(to bottom, #440154, #440154, #31678E, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387)"
</div>
"""
m.get_root().html.add_child(folium.Element(legend_html))

# Add layer control
folium.LayerControl().add_to(m)
def create_popup_table(row):
    return folium.Popup(
        f"""
        <table>
            <tr><th>Latitude</th><td>{row['latitude_grid']}</td></tr>
            <tr><th>Longitude</th><td>{row['longitude_grid']}</td></tr>
            <tr><th>Grid</th><td>{row['grid']}</td></tr>
            <tr><th>Count</th><td>{row['count']}</td></tr>
        </table>
        """,
        max_width=250
    )


for index, row in grid_counts.iterrows():
    folium.Circle(
        location=[row['latitude_grid'], row['longitude_grid']],
        radius=50, # Adjust the radius as needed
        color='blue',
        fill=True,
        fill_color='blue',
        fill_opacity=0,
        opacity=0,
        popup=create_popup_table(row)
    ).add_to(m)

m.show_in_browser()
