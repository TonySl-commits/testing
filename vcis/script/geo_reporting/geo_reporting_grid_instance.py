import pandas as pd
import numpy as np
import datapane as dp
import plotly.graph_objects as go
import matplotlib.pyplot as plt
df = pd.read_csv('df_merged_list.csv')



print(df.columns)

df = df[['usage_timeframe','device_id_x','latitude_grid_x','longitude_grid_x', 'device_id_y','latitude_grid_y','longitude_grid_y']]
# df['day'] = pd.to_datetime(df['usage_timeframe']).dt.date
print(df.head(30))


import plotly.express as px

# Assuming your DataFrame is named 'df'
# Create a new column to identify whether latitude and longitude are the same
df['same_location'] = (df['latitude_grid_x'] == df['latitude_grid_y']) & (df['longitude_grid_x'] == df['longitude_grid_y'])

# Create the 2D histogram scatter plot using Plotly with facet_col
fig = px.scatter(df, x='longitude_grid_x', y='latitude_grid_x', color='same_location',
                symbol='same_location', symbol_map={True: 'circle', False: 'cross'},
                labels={'same_location': 'Same Location'},
                facet_col='device_id_y',  # Use facet_col to create subplots for each device
                title="Mapping Shared Instances Across Time")

# Update symbol and color attributes for better visibility
fig.update_traces(marker=dict(size=8, opacity=0.8), selector=dict(mode='markers'))
fig.update_layout(title_text='Mapping Shared Instances Across Time',
                title_x=0.5,  # Set title_x to 0.5 to center the title horizontally
                title_y=0.95) 

#################################################################################################################
from plotly.subplots import make_subplots

# Get unique device IDs
unique_device_ids = df['device_id_y'].unique()

# Create subplots
fig2 = make_subplots(rows=len(unique_device_ids), cols=1, subplot_titles=[f'Device {device_id}' for device_id in unique_device_ids])
traces = []
for i, device_id in enumerate(unique_device_ids, start=1):
    df_device = df[df['device_id_y'] == device_id]

    x = df_device['latitude_grid_x']
    y = df_device['longitude_grid_x']

    grid_x = [[row['latitude_grid_x'], row['longitude_grid_x']] for index, row in df_device.iterrows()]
    grid_y = [[row['latitude_grid_y'], row['longitude_grid_y']] for index, row in df_device.iterrows()]

    equal_positions = [grid_x[i] for i in range(len(grid_x)) if grid_x[i] == grid_y[i]]
    not_equal_positions = [grid_y[i] for i in range(len(grid_x)) if grid_x[i] != grid_y[i]]

    equal_positions_x = [equal_positions[i][0] for i in range(len(equal_positions))]
    equal_positions_y = [equal_positions[i][1] for i in range(len(equal_positions))]
    not_equal_positions_x = [not_equal_positions[i][0] for i in range(len(not_equal_positions))]
    not_equal_positions_y = [not_equal_positions[i][1] for i in range(len(not_equal_positions))]

    # Add separate traces for each device
    trace_1 = go.Scatter(
            x=not_equal_positions_x,
            y=not_equal_positions_y,
            mode='markers',
            showlegend=False,
            name=f'device_id {device_id}',
            marker=dict(
                symbol='x',
                opacity=0.7,
                size=8,
                line=dict(width=1),
            )
        )

    trace_2  = go.Scatter(
        x=equal_positions_x,
        y=equal_positions_y,
        mode='markers',
        showlegend=False,
        name=f'device_id {device_id}',
        marker=dict(
            symbol='circle',
            opacity=0.7,
            size=8,
            line=dict(width=1),
        )
    )

    trace_3  = go.Histogram2d(
        x=x,
        y=y,
        colorscale='YlGnBu',
        zmax=10,
        nbinsx=14,
        nbinsy=14,
        zauto=False,
        name=f'device_id {device_id}'
    )
    traces.extend([trace_1, trace_2, trace_3])
buttons = []
fig_2 = go.Figure(data=traces)

fig_2.update_layout(
    barmode='group',
    xaxis=dict(title='Device ID'),
    yaxis=dict(title='Values'),
    title='Mapping Shared Instances Across Time',
    legend=dict(title='Metrics', orientation='v', y=1.1, x=1.02)
)

# Add "Select Month" as the first option in the dropdown
dropdown_menu = [{"label": "Select Device", "method": "relayout", "args": ["title", 'Mapping Shared Instances Across Time']}]

for i, device_id in enumerate(unique_device_ids, start=1):
    is_visible = [False] * len(fig_2.data)
    indices = [i for i, trace in enumerate(fig_2.data) if f'device_id {device_id}' in trace.name]
    for idx in indices:
        is_visible[idx] = True

    button = dict(label=str(device_id),
                method='update',
                args=[{'visible': is_visible}])
    buttons.append(button)

fig_2.update_layout(
    updatemenus=[
        dict(
            buttons=dropdown_menu + buttons,  
            direction='down',
            showactive=True,
            x=0.5,
            xanchor='center',
            y=1.2,
            yanchor='top'
        )
    ]
)


# fig2.update_layout(
#     height=600 * len(unique_device_ids),
#     width=600,
#     showlegend=False,
# )


# import pandas as pd
# import plotly.graph_objects as go
# from plotly.subplots import make_subplots
# def plot_creation(df, output_html_path):
#     # Ensure 'device_id' is unique
#     df['device_id'] = df['device_id'].astype(str) + '_' + df.index.astype(str)

#     traces = []

#     for month in sorted(df['Month'].unique()):
#         initial_months = df[df['Month'] == month]['device_id'].unique()
#         initial_data = df[(df['Month'] == month) & (df['device_id'].isin(initial_months))]

#         trace_avg_time_spent = go.Bar(
#             name=f'Avg Time - Month {month}',
#             x=initial_data['device_id'],
#             y=initial_data['AVGtime_spent_per_visit'],
#             marker=dict(color='midnightblue'),
            
#             hoverinfo='text+y',
#             legendgroup='group1'  # Assigning a common legend group
#         )

#         trace_num_visits = go.Bar(
#             name=f'Visits - Month {month}',
#             x=initial_data['device_id'],
#             y=initial_data['Visits_per_month'],
#             marker=dict(color='cornflowerblue'),
            
#             hoverinfo='text+y',
#             legendgroup='group2'  # Assigning a common legend group
#         )

#         trace_freq_score = go.Scatter(
#             name=f'Freq Visitors Score - Month {month}',
#             x=initial_data['device_id'],
#             y=initial_data['Freq_Visitors_Score'],
#             mode='lines+markers',
#             line=dict(color='indianred', width=2),
            
#             hoverinfo='text+y',
#             legendgroup='group3'  # Assigning a common legend group
#         )

#         traces.extend([trace_avg_time_spent, trace_num_visits, trace_freq_score])

#     fig_combined = go.Figure(data=traces)

#     fig_combined.update_layout(
#         barmode='group',
#         xaxis=dict(title='Device ID'),
#         yaxis=dict(title='Values'),
#         title='Frequency Visitors Score',
#         legend=dict(title='Metrics', orientation='v', y=1.1, x=1.02)
#     )

#     buttons = []

#     # Add "Select Month" as the first option in the dropdown
#     dropdown_menu = [{"label": "Select Month", "method": "relayout", "args": ["title", "Frequency Visitors Analysis"]}]

#     for month in sorted(df['Month'].unique()):
#         is_visible = [False] * len(fig_combined.data)
#         indices = [i for i, trace in enumerate(fig_combined.data) if f'Month {month}' in trace.name]
#         for idx in indices:
#             is_visible[idx] = True

#         button = dict(label=str(month),
#                       method='update',
#                       args=[{'visible': is_visible}])
#         buttons.append(button)

#     fig_combined.update_layout(
#         updatemenus=[
#             dict(
#                 buttons=dropdown_menu + buttons,  
#                 direction='down',
#                 showactive=True,
#                 x=0.5,
#                 xanchor='center',
#                 y=1.2,
#                 yanchor='top'
#             )
#         ]
#     )


###############################################################################################################

banner_html = """<div style="padding-left: 170px;font-size: 22px;display: flex;color: #312E81;">
    <h1>GEO Data Analysis</h1>
    </div>
    """
header = dp.Group(dp.Media("./valoores.png"), dp.HTML(banner_html), columns=2, widths=[1, 5])
devider = dp.HTML("""<hr style="border: none; border-top: 5px solid red; width: 100%; margin-left: auto; margin-right: auto;">""")
# layer1 = dp.HTML("""<div style="padding-left: 100px;font-size: 10px;display: flex;color: #3C3633;">
#     <h1>Device Grid Comparison</h1>
#     </div>
#     """)
layer2 = dp.Plot(fig)
layer3 = dp.Plot(fig2)
blocks = [devider ,layer2,layer3]
report = dp.View(
    header,
    dp.Group(
        blocks=blocks  
    ),
)
dp.save_report(path = 'my_report.html',blocks = report ,open=True)








