import pandas as pd
import plotly.graph_objs as go

# Load data from CSV files
d1 = pd.read_csv('test1.csv')
d2 = pd.read_csv('test2.csv')

# Separate main device data and other devices data
main_device_id = d1['device_id'].iloc[0]
main_device_data = d1.set_index('all_dates')['count'].to_dict()
other_devices_data = d2.groupby(['device_id', 'all_dates'])['count'].sum().reset_index()
print(main_device_data)
# Set the order of days
days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# Convert 'all_dates' to categorical data type with ordered categories
other_devices_data['all_dates'] = pd.Categorical(other_devices_data['all_dates'], categories=days_order, ordered=True)
main_device_data_squence = {k: v for k, v in sorted(main_device_data.items(), key=lambda item: days_order.index(item[0]))}

# Set the bar width
num_other_devices = other_devices_data['device_id'].nunique()
main_bar_width = 0.6
other_bar_width = main_bar_width / num_other_devices

# Color palette
palette = ["#fee090", "#fdae61", "#4575b4", "#313695", "#e0f3f8", "#abd9e9", "#d73027", "#a50026"]

# Create the trace for the main device bar
trace_main = go.Bar(
    x=list(main_device_data_squence.keys()),
    y=list(main_device_data_squence.values()),
    width=main_bar_width,
    name=main_device_id,
    marker_color=palette[0]  # Set the color for the main device bar
)

# Create traces for other devices bars
traces_other = []
i = 0
for device_id, group in other_devices_data.groupby('device_id'):
    trace = go.Bar(
        x=group['all_dates'],
        y=group['count'],
        width=other_bar_width,
        base=0,
        name=device_id,
        offset=-main_bar_width/2 + (i + 1) * other_bar_width,
        marker_color=palette[i + 1]  # Set the color for each other device bar
    )
    traces_other.append(trace)
    i += 1

# Create the layout
layout = go.Layout(
    barmode='group',
    title='Device Data',
    xaxis=dict(title='Day of Week'),
    yaxis=dict(title='Count')
)

# Create the figure and plot
fig = go.Figure(data=[trace_main] + traces_other, layout=layout)
fig.show()