import pandas as pd
import json
import plotly.graph_objects as go
import plotly.express as px
from vcis.utils.properties import CDR_Properties

properties = CDR_Properties()
# data = json.load(open(r"C:\Users\joseph.as\Desktop\Takeout\Location History\Records.json"))
# df_device = pd.json_normalize(data['locations'])

# df_device['latitude'] = df_device['latitudeE7'] / 10000000
# df_device['longitude'] = df_device['longitudeE7'] / 10000000
# df = df_device[['longitude','latitude','timestamp']]
# df['timestamp'] = pd.to_datetime(df['timestamp'],format='ISO8601')
print('1')
df = pd.read_csv(properties.passed_filepath_excel + "df_device.csv")
print('2')
df['timestamp'] = df['usage_timeframe']
df['latitude'] = df['location_latitude']
df['longitude'] = df['location_longitude'] 

df = df.sort_values('usage_timeframe')
print('3')
# Create a boolean mask for rows where the coordinates are the same as the previous and next rows
mask = (df['location_latitude'] == df['location_latitude'].shift())  & (df['location_longitude'] == df['location_longitude'].shift()) 
print('4')
# Drop rows where the mask is True
df = df[~mask]
print('5')
fig = px.scatter_mapbox(df, lon="longitude", lat="latitude", hover_name='timestamp',
                      color_discrete_sequence=["fuchsia"], zoom=3,animation_frame="timestamp")
print('6')
fig.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 600
fig.layout.updatemenus[0].buttons[0].args[1]["transition"]["duration"] = 600
fig.layout.sliders[0].pad.t = 10
fig.layout.updatemenus[0].pad.t= 10
print('7')
fig.update_layout(mapbox_style="open-street-map")
print('8')
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
print('9')
fig.show()

