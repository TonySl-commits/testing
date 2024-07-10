import pandas as pd
import numpy as np
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, RangeTool, DataTable, TableColumn, CustomJS, LinearAxis, Range1d, DateFormatter, Title, Div, Scatter
from bokeh.plotting import figure, show, output_file
from bokeh.models.tiles import WMTSTileSource

# Load data
data = pd.read_csv('C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/co_traveller/df_common.csv')
data.drop(columns=['Unnamed: 0', 'main_id', 'latitude_grid', 'longitude_grid', 'grid'], inplace=True)
data['date'] = pd.to_datetime(data['usage_timeframe'], unit='ms')

# Convert latitude and longitude to Web Mercator coordinates
def wgs84_to_web_mercator(lat, lon):
    k = 6378137
    x = lon * (k * np.pi/180.0)
    y = np.log(np.tan((90 + lat) * np.pi/360.0)) * k
    return (x, y)

data['x'], data['y'] = wgs84_to_web_mercator(data['location_latitude'], data['location_longitude'])

# Aggregate the data
agg_data = data.groupby(data['date'].dt.date).agg(
    hits=('device_id', 'count'),
    unique_devices=('device_id', 'nunique')
).reset_index()
agg_data['date'] = pd.to_datetime(agg_data['date'])

source = ColumnDataSource(agg_data)
map_source = ColumnDataSource(data)

# Set up plot styling
title_props = {'text_font': "Bahnschrift SemiBold", 'text_font_size': '15pt', 'text_color': '#03045E'}

# Main plot
p = figure(height=300, width=1500, tools="xpan", toolbar_location=None, x_axis_type="datetime", x_axis_location="below",
           background_fill_color="white", x_range=(agg_data['date'][0], agg_data['date'][len(agg_data) // 2]),
           y_range=(0, agg_data['hits'].max()*1.2))
p.add_layout(Title(text="Device Hits and Unique Devices", **title_props), 'above')

p.scatter('date', 'hits', source=source, color="darkblue", size=8, legend_label="Hits")
p.scatter('date', 'unique_devices', source=source, color="red", size=8, y_range_name="unique_devices", legend_label="Unique Devices")

p.extra_y_ranges = {"unique_devices": Range1d(start=0, end=agg_data['unique_devices'].max()*1.2)}
p.add_layout(LinearAxis(y_range_name="unique_devices"), 'right')

# Range selection plot
select = figure(height=130, width=1500, y_range=p.y_range, x_axis_type="datetime", y_axis_type=None, tools="", toolbar_location=None, background_fill_color="white")
select.add_layout(Title(text="Drag the middle and edges of the selection box to change the range above", **title_props), 'above')
range_tool = RangeTool(x_range=p.x_range)
range_tool.overlay.fill_color = "gray"
range_tool.overlay.fill_alpha = 0.5
select.add_tools(range_tool)

# Data table with Div for title
data_table = DataTable(source=source, columns=[
    TableColumn(field="date", title="Date", formatter=DateFormatter()),
    TableColumn(field="device_id", title="Device ID"),
    TableColumn(field="location_latitude", title="Latitude"),
    TableColumn(field="location_longitude", title="Longitude")
], width=1500, height=280)
data_table_div = Div(text="<h3 style='text-align:center;'>Detailed Device Data</h3>")

# Map plot
tile_provider = WMTSTileSource(url="https://c.tile.openstreetmap.org/{Z}/{X}/{Y}.png")
map_plot = figure(title="Geospatial Data", x_axis_type="mercator", y_axis_type="mercator",
                  width=1500, height=600, tools="pan,wheel_zoom,box_zoom,reset", active_scroll="wheel_zoom")
map_plot.add_tile(tile_provider)
map_plot.scatter(x='x', y='y', size=10, color="blue", fill_alpha=0.8, source=map_source)
map_plot.xaxis.visible = False
map_plot.yaxis.visible = False
map_plot.add_layout(Title(text="Geospatial Data", **title_props), 'above')

# JavaScript callback to update table and map based on selected range
callback = CustomJS(args=dict(source=source, map_source=map_source), code="""
    var data = source.data;
    var map_data = map_source.data;
    var start = cb_obj.start;
    var end = cb_obj.end;

    for (var key in data) {
        data[key] = [];
    }

    for (var key in map_data) {
        map_data[key] = [];
    }

    for (var i = 0; i < source.get_length(); i++){
        if (data['date'][i] >= start && data['date'][i] <= end){
            for (var key in data) {
                data[key].push(source.data[key][i]);
            }
            for (var key in map_data) {
                map_data[key].push(map_source.data[key][i]);
            }
        }
    }
    source.change.emit();
    map_source.change.emit();
""")

select.js_on_change('x_range', callback)
p.x_range.js_on_change('start', callback)
p.x_range.js_on_change('end', callback)

# Combine plots, table, and map
layout = column(p, select, data_table_div, data_table, map_plot)

# Output
output_file("interactive_dashboard.html", title="Interactive Dashboard")
show(layout)
