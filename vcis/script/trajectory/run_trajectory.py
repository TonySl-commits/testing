
import pandas as pd
import plotly.graph_objects as go

df_device = pd.read_excel("/u01/csvfiles/CDR_trace/data/record_result/joseph.xlsx")
df_device['closest_point'] = df_device['closest_point'].astype(str)
df_device['lat'] = df_device['closest_point'].apply(lambda x: x.replace("(", "").replace(")", "").split(",")[0].strip())

df_device['lon']= df_device['closest_point'].apply(lambda x: x.replace("(", "").replace(")", "").split(",")[-1].strip())

df_device = df_device.sort_values('usage_timeframe')
mask = (df_device['location_latitude'] == df_device['location_latitude'].shift())  & (df_device['location_longitude'] == df_device['location_longitude'].shift()) 
df_device = df_device[~mask]
print(df_device.shape)
trace_device = go.Scattermapbox(
    lat=df_device['lat'],
    lon=df_device['lon'],
    mode='markers',
    marker=dict(
        size=10,
        color='blue',
        opacity=0.1
    ),
    hovertext=df_device['imsi_id'],
    name='JOSEPH'
)


# Create the layout for the map
layout = go.Layout(
    title='Device and Co-traveler Locations',
    mapbox=dict(
        style='open-street-map',
        zoom=10
    ),
    showlegend=True
)

# Create the figure and add the traces
fig = go.Figure(data=trace_device, layout=layout)

# Show the figure
fig.show()

