import pandas as pd
import json
import plotly.graph_objects as go
import plotly.express as px
data = json.load(open(r"C:\Users\joseph.as\Desktop\Takeout\Location History\Records.json"))
print(data)
df_device = pd.json_normalize(data['locations'])
print(df_device)

df_device['latitude'] = df_device['latitudeE7'] / 10000000
df_device['longitude'] = df_device['longitudeE7'] / 10000000

# print(df_device[['longitude','latitude','timestamp']])
# trace_device = go.Scattermapbox(
#     lat=df_device['latitude'],
#     lon=df_device['longitude'],
#     mode='markers',
#     marker=dict(
#         size=10,
#         color='blue',
#         opacity=0.7
#     ),
#     name='RIAD'
# )

# # Create the layout for the map
# layout = go.Layout(
#     title='Device and Co-traveler Locations',
#     mapbox=dict(
#         style='open-street-map',
#         zoom=10
#     ),
#     showlegend=True
# )

# # Create the figure and add the traces
# fig = go.Figure(data=trace_device, layout=layout)

# # Show the figure
# fig.show()


fig = px.scatter_mapbox(df_device, lat='latitude', lon='longitude', hover_name='timestamp',
                      color_discrete_sequence=["fuchsia"], zoom=3, height=300)
fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()