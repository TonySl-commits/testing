import pandas as pd
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, RangeTool, DataTable, TableColumn, CustomJS, DateRangeSlider, HoverTool
from bokeh.plotting import figure, show, output_file

# Load the data globally
data = pd.read_csv('C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/co_traveller/df_common.csv')

data.drop(columns=['Unnamed: 0', 'main_id', 'latitude_grid', 'longitude_grid', 'grid'], inplace=True)


# data.rename(columns={'usage_timeframe': 'Date',
#                     'location_latitude' : 'Latitude', 
#                      'location_longitude': 'Longitude',
#                      'device_id': 'Device ID',
#                      'location_name': 'Location Name',
#                      'service_provider_id': 'Service Provider ID'}, inplace=True)

# data['Date'] = pd.to_datetime(data['Date'], unit='ms')

# # Aggregate the data for the line chart
# agg_data = data.groupby(data['Date'].dt.date).agg(
#     hits=('Device ID', 'count'),
#     unique_devices=('Device ID', 'nunique')
# ).reset_index()

data['date'] = pd.to_datetime(data['usage_timeframe'], unit='ms')

# Aggregate the data for the line chart
agg_data = data.groupby(data['date'].dt.date).agg(
    hits=('device_id', 'count'),
    unique_devices=('device_id', 'nunique')
).reset_index()

# Convert `date` column to datetime in agg_data for compatibility
agg_data['date'] = pd.to_datetime(agg_data['date'])

source = ColumnDataSource(agg_data)
table_source = ColumnDataSource(agg_data)

# Main plot
p = figure(height=300, width=800, tools="xpan", toolbar_location=None,
           x_axis_type="datetime", x_axis_location="above",
           background_fill_color="#efefef", x_range=(agg_data['date'][0], agg_data['date'][len(agg_data) // 2]))

p.line('date', 'hits', source=source, legend_label="Hits", color="blue")
p.line('date', 'unique_devices', source=source, legend_label="Unique Devices", color="green", y_range_name="unique_devices")

p.yaxis.axis_label = 'Hits'

# Range selection plot
select = figure(title="Drag the middle and edges of the selection box to change the range above",
                height=130, width=800, y_range=p.y_range,
                x_axis_type="datetime", y_axis_type=None,
                tools="", toolbar_location=None, background_fill_color="#efefef")

range_tool = RangeTool(x_range=p.x_range)
range_tool.overlay.fill_color = "navy"
range_tool.overlay.fill_alpha = 0.2

select.line('date', 'hits', source=source, color="blue")
select.line('date', 'unique_devices', source=source, color="green")
select.ygrid.grid_line_color = None
select.add_tools(range_tool)
select.toolbar.active_multi = range_tool

# Data table
columns = [
    TableColumn(field="date", title="Date"),
    TableColumn(field="hits", title="Hits"),
    TableColumn(field="unique_devices", title="Unique Devices")
]
data_table = DataTable(source=table_source, columns=columns, width=800, height=280)

# JavaScript callback to update table based on selected range
callback = CustomJS(args=dict(source=source, table_source=table_source), code="""
    var data = source.data;
    var table_data = table_source.data;
    var start = cb_obj.value[0];
    var end = cb_obj.value[1];

    table_data['date'] = [];
    table_data['hits'] = [];
    table_data['unique_devices'] = [];

    for (var i = 0; i < data['date'].length; i++) {
        var date = new Date(data['date'][i]).getTime();
        if (date >= start && date <= end) {
            table_data['date'].push(data['date'][i]);
            table_data['hits'].push(data['hits'][i]);
            table_data['unique_devices'].push(data['unique_devices'][i]);
        }
    }
    table_source.change.emit();
""")
p.x_range.js_on_change('start', callback)
p.x_range.js_on_change('end', callback)

# Combine plots and table
layout = column(p, select, data_table)

# Output to HTML file
output_file("interactive_dashboard.html", title="Interactive Dashboard")
show(layout)
