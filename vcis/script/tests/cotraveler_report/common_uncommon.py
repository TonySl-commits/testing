import pandas as pd
from vcis.utils.utils import CDR_Utils
import plotly.graph_objects as go
import numpy as np
utils = CDR_Utils()

df_history = pd.read_csv('src/data/excel/df_history.csv')
df_common = pd.read_csv('src/data/excel/df_common.csv')
pd.set_option('display.max_columns', None)
df_common = utils.convert_datetime_to_ms(df_common)

df_common = df_common[['device_id', 'location_latitude', 'location_longitude','usage_timeframe','grid']]
df_history = df_history[['device_id', 'location_latitude', 'location_longitude','usage_timeframe','grid']]

x=0
for device_id in df_history['device_id'].unique():
    df_device = df_history[df_history['device_id'] == device_id]
    df_device_common = df_common[df_common['device_id'] == device_id]
    if x==2:
        break
    x=x+1

df_main = pd.read_csv('src/data/excel/df_device.csv')


def plot1(df_main,df_common,df_history):

    fig = go.Figure()

    fig.add_trace(go.Scattermapbox(
        lat=df_main['location_latitude'],
        lon=df_main['location_longitude'],
        mode='markers',
        marker=go.scattermapbox.Marker(
            size=5,
            color='#ed5151',
            opacity=0.5
        ),
        text=df_main['device_id'], 
        name='Main Device'
    ))

    # Add a trace for df_device
    fig.add_trace(go.Scattermapbox(
        lat=df_device['location_latitude'],
        lon=df_device['location_longitude'],
        mode='markers',
        marker=go.scattermapbox.Marker(
            size=5,
            color='#149ece',
            opacity=0.5
        ),
        text="Cotraveler Device", 
        name=f'Cotraveler Device'
    ))

    for index, row in df_device_common.iterrows():
        lat, lon = map(float, row['grid'].split(','))
        lat_min, lon_min, lat_max, lon_max = utils.calculate_box_boundaries(lat, lon, 50)
        
        # Draw a box around the point
        fig.add_trace(go.Scattermapbox(
            lat=[lat_min, lat_max, lat_max, lat_min, lat_min],
            lon=[lon_min, lon_min, lon_max, lon_max, lon_min],
            mode='lines',
            line=dict(width=3, color='#9e559c'),
            showlegend=False
        ))

    # Set the map style
    fig.update_layout(
        autosize=True,
        hovermode='closest',
        mapbox=dict(
            style='open-street-map',
            bearing=0,
            center=dict(
                lat=df_main['location_latitude'].mean(), 
                lon=df_main['location_longitude'].mean()
            ),
            pitch=0,
            zoom=10
        ),
    )

    # Show the figure
    fig.show()

    return fig


plot1(df_main,df_common,df_history)