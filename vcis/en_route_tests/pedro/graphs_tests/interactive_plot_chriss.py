import pandas as pd
import numpy as np
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, RangeTool, DataTable, TableColumn, CustomJS, LinearAxis, Range1d, DateFormatter
from bokeh.plotting import figure, show, output_file

# Load the data globally
data = pd.read_csv('C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/co_traveller/df_common.csv')

data.drop(columns=['Unnamed: 0', 'main_id', 'latitude_grid', 'longitude_grid', 'grid'], inplace=True)

data['date'] = pd.to_datetime(data['usage_timeframe'], unit='ms')

# Aggregate the data for the line chart
agg_data = data.groupby(data['date'].dt.date).agg(
    hits=('device_id', 'count'),
    unique_devices=('device_id', 'nunique')
).reset_index()

# Convert `date` column to datetime in agg_data for compatibility
agg_data['date'] = pd.to_datetime(agg_data['date'])

source = ColumnDataSource(agg_data)
original_source = ColumnDataSource(data)
table_source = ColumnDataSource(data)

# Main plot
p = figure(height=300, width=800, tools="xpan", toolbar_location=None,
           x_axis_type="datetime", x_axis_location="above",
           background_fill_color="#efefef", x_range=(agg_data['date'][0], agg_data['date'][len(agg_data) // 2]))

p.line('date', 'hits', source=source, legend_label="Hits", color="blue")
p.extra_y_ranges = {"unique_devices": Range1d(start=0, end=agg_data['unique_devices'].max())}
p.add_layout(LinearAxis(y_range_name="unique_devices"), 'right')
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
select.extra_y_ranges = {"unique_devices": p.extra_y_ranges["unique_devices"]}
select.line('date', 'unique_devices', source=source, color="green", y_range_name="unique_devices")
select.ygrid.grid_line_color = None
select.add_tools(range_tool)

# Data table
columns = [
    TableColumn(field="date", title="Date", formatter=DateFormatter()),
    TableColumn(field="device_id", title="Device ID"),
    TableColumn(field="location_latitude", title="Latitude"),
    TableColumn(field="location_longitude", title="Longitude")
]
data_table = DataTable(source=table_source, columns=columns, width=800, height=280)

# JavaScript callback to update table based on selected range
callback = CustomJS(args=dict(original_source=original_source, table_source=table_source), code="""
    var original_data = original_source.data;
    var table_data = table_source.data;
    var start = cb_obj.start;
    var end = cb_obj.end;

    table_data['date'] = [];
    table_data['device_id'] = [];
    table_data['location_latitude'] = [];
    table_data['location_longitude'] = [];

    for (var i = 0; i < original_data['date'].length; i++) {
        var date = new Date(original_data['date'][i]).getTime();
        if (date >= start && date <= end) {
            table_data['date'].push(original_data['date'][i]);
            table_data['device_id'].push(original_data['device_id'][i]);
            table_data['location_latitude'].push(original_data['location_latitude'][i]);
            table_data['location_longitude'].push(original_data['location_longitude'][i]);
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