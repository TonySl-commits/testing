import pandas as pd
import plotly.graph_objects as go
from scipy.interpolate import interp1d
import pandas as pd
from vcis.utils.utils import CDR_Utils
import plotly.graph_objects as go
import numpy as np
utils = CDR_Utils()

df_history = pd.read_csv('data/excel/df_history_backup.csv')
df_common = pd.read_csv('data/excel/df_common_backup.csv')
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

df_main = pd.read_csv('data/excel/df_device_backup.csv')


# Assuming df_device and df_device_common are your DataFrames for the devices
# and they have 'location_latitude' and 'location_longitude' columns

# Function to interpolate the data
def interpolate_data(df, num_points=30):
    # Assuming 'timestamp' column exists in df
    df_sorted = df.sort_values(by='usage_timeframe')
    lat_interp = interp1d(df_sorted.index, df_sorted['location_latitude'], kind='linear')
    lon_interp = interp1d(df_sorted.index, df_sorted['location_longitude'], kind='linear')
    
    new_index = np.linspace(df_sorted.index.min(), df_sorted.index.max(), num_points)
    new_df = pd.DataFrame({
        'location_latitude': lat_interp(new_index),
        'location_longitude': lon_interp(new_index)
    }, index=new_index)
    
    return new_df

# Interpolate the data for both devices
df_device_interp = interpolate_data(df_device)
df_device_common_interp = interpolate_data(df_device_common)

# Reset the index of the interpolated DataFrames
df_device_interp = df_device_interp.reset_index(drop=True)
df_device_common_interp = df_device_common_interp.reset_index(drop=True)

# Create frames for animation
frames = []
for i in range(len(df_device_interp)):
    frame = go.Frame(data=[
        go.Scattermapbox(
            lat=[df_device_interp.loc[i, 'location_latitude']],
            lon=[df_device_interp.loc[i, 'location_longitude']],
            mode='markers',
            marker=dict(size=6, color='#149ece', opacity=0.8),
            text="Cotraveler Device",
            name=f'Cotraveler Device'
        ),
        go.Scattermapbox(
            lat=[df_device_common_interp.loc[i, 'location_latitude']],
            lon=[df_device_common_interp.loc[i, 'location_longitude']],
            mode='markers',
            marker=dict(size=6, color='#ed5151', opacity=0.8),
            text="Main Device",
            name='Main Device'
        )
    ])
    frames.append(frame)


# Create the figure with frames
fig = go.Figure(
    data=[go.Scattermapbox(
        lat=[df_device_interp.loc[0, 'location_latitude']],
        lon=[df_device_interp.loc[0, 'location_longitude']],
        mode='markers',
        marker=dict(size=6, color='#149ece', opacity=0.8),
        text="Cotraveler Device",
        name=f'Cotraveler Device'
    ),
    go.Scattermapbox(
        lat=[df_device_common_interp.loc[0, 'location_latitude']],
        lon=[df_device_common_interp.loc[0, 'location_longitude']],
        mode='markers',
        marker=dict(size=6, color='#ed5151', opacity=0.8),
        text="Main Device",
        name='Main Device'
    )],
    frames=frames,
    layout=go.Layout(
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
        updatemenus=[dict(
            type="buttons",
            showactive=False,
            buttons=[dict(label="Play",
                          method="animate",
                          args=[None, {"frame": {"duration": 500, "redraw": True},
                                        "fromcurrent": True,
                                        "transition": {"duration": 300,
                                                       "easing": "quadratic-in-out"}}])])]
    )
)

# Show the figure
fig.show()
