import plotly.express as px
import pandas as pd
import plotly.graph_objects as go
from vcis.utils.utils import CDR_Utils, CDR_Properties

properties= CDR_Properties()
df = pd.read_csv(properties.passed_filepath_excel+'df_history_backup.csv')
# 2A0CA0C9-DD9A-4BDA-A914-5B80FA7AD77E
# 22a9f8a3-127e-49e3-a719-79431e48fd81

df = df[(df['device_id'] == '2A0CA0C9-DD9A-4BDA-A914-5B80FA7AD77E') | (df['device_id'] == '22a9f8a3-127e-49e3-a719-79431e48fd81')]

# # Sample data
# data = {
#     'device_id': ['device1', 'device1', 'device1', 'device2', 'device2', 'device2'],
#     'location_latitude': [37.7749, 37.7750, 37.7751, 37.7748, 37.7749, 37.7750],
#     'location_longitude': [-122.4194, -122.4195, -122.4196, -122.4193, -122.4194, -122.4195],
#     'usage_timeframe': ['2024-04-01 08:00:00', '2024-04-01 09:00:00', '2024-04-01 10:00:00',
#                         '2024-04-01 08:00:00', '2024-04-01 09:00:00', '2024-04-01 10:00:00']
# }

# df = pd.DataFrame(data)
# Function to create a trail effect
def create_trail_effect(df, window_size=10):
    trails = []
    for device_id in df['device_id'].unique():
        device_df = df[df['device_id'] == device_id]
        for i in range(0, len(device_df), window_size):
            trail = device_df.iloc[i:i+window_size]
            trails.append(trail)
    return pd.concat(trails)

# Create the trail effect
trail_df = create_trail_effect(df)

# Plotting the trajectories with the trail effect
fig = go.Figure()

# Define a color mapping for device IDs
color_mapping = {'2A0CA0C9-DD9A-4BDA-A914-5B80FA7AD77E': '#ff0000', '22a9f8a3-127e-49e3-a719-79431e48fd81': '#0000ff'}

for device_id in trail_df['device_id'].unique():
    device_df = trail_df[trail_df['device_id'] == device_id]
    fig.add_trace(go.Scattergeo(
        lat=device_df['location_latitude'],
        lon=device_df['location_longitude'],
        mode='lines',
        line=dict(width=2, color=color_mapping[device_id]), # Use the color mapping here
        name=device_id
    ))

fig.update_layout(
    title='Device Trajectories with Trail Effect',
    mapbox_style="open-street-map"
)
# Saving the map as a static HTML file
fig.write_html('device_trajectories_trail.html', auto_open=True)