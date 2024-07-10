import pandas as pd
import plotly.express as px
import plotly.graph_objs as go

# Path to the data file
df_path = 'C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/co_traveller/df_common.csv'

# Load the data
df = pd.read_csv(df_path)

# Convert milliseconds since epoch to datetime
df['timestamp'] = pd.to_datetime(df['usage_timeframe'], unit='ms')

# Create the initial figure with time series data
fig = go.Figure()

# Add trace for the line chart showing hits over time (count of hits by timestamp)
hits_data = df.groupby('timestamp').size().reset_index(name='count')
fig.add_trace(
    go.Scatter(x=hits_data['timestamp'], y=hits_data['count'], mode='lines+markers', name='Device Hits')
)

# # Add trace for the geographic scatter plot showing locations
# fig.add_trace(
#     go.Scattergeo(lon=df['location_longitude'], lat=df['location_latitude'], mode='markers', name='Location Hits')
# )

# Configure the layout to include a slider and buttons for time range selection
fig.update_layout(
    title='Interactive Plot of Device Hits and Locations',
    xaxis=dict(
        rangeselector=dict(
            buttons=list([
                dict(count=1, label='1d', step='day', stepmode='backward'),
                dict(count=7, label='1w', step='day', stepmode='backward'),
                dict(count=1, label='1m', step='month', stepmode='backward'),
                dict(step='all')
            ])
        ),
        rangeslider=dict(visible=True),
        type='date'
    ))
#     geo=dict(
#         projection_type='natural earth',
#         showland=True,
#         landcolor='lightgreen'
#     )
# )

# Save the plot to an HTML file
fig.write_html('C:/Users/mpedro/Desktop/interactive_plot.html', auto_open=True)
