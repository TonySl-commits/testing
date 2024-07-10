import pandas as pd
import plotly.graph_objects as go
from movingpandas import Trajectory
import geopandas as gpd
from geopandas import GeoSeries
import movingpandas as mpd
import contextily as ctx
from shapely.geometry import Point
import matplotlib.pyplot as plt 


df_device = pd.read_excel("/u01/csvfiles/CDR_trace/data/record_result/joseph.xlsx")
df_device['closest_point'] = df_device['closest_point'].astype(str)
df_device['lat'] = df_device['closest_point'].apply(lambda x: x.replace("(", "").replace(")", "").split(",")[0].strip())

df_device['lon']= df_device['closest_point'].apply(lambda x: x.replace("(", "").replace(")", "").split(",")[-1].strip())

df_device = df_device.sort_values('usage_timeframe')
mask = (df_device['location_latitude'] == df_device['location_latitude'].shift())  & (df_device['location_longitude'] == df_device['location_longitude'].shift()) 
df_device = df_device[~mask]
print(df_device.shape)

df_device['usage_timeframe'] = pd.to_datetime(df_device['usage_timeframe']) # Ensure 'usage_timeframe' is in datetime format
df_device.set_index('usage_timeframe', inplace=True)


gdf = gpd.GeoDataFrame(df_device, geometry = gpd.points_from_xy(df_device['location_longitude'], df_device['location_latitude']), crs = 'EPSG:4326')


print(gdf.head())
# Create a Trajectory object
traj = mpd.Trajectory(gdf, 1)

fig, ax = plt.subplots(figsize = (25, 25), dpi = 200)
traj.plot(ax = ax)
ctx.add_basemap(ax, crs = 'EPSG:4326')
plt.show()
# Save the plot as an HTML file
plt.plot(fig, filename='plot.html', auto_open=False)



