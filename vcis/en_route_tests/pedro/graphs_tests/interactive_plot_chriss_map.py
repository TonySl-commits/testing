import pandas as pd
import numpy as np
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, RangeTool, Div, DataTable, TableColumn, CustomJS, LinearAxis, Range1d, DateFormatter, Title
from bokeh.plotting import figure, show, output_file
from bokeh.tile_providers import get_provider, Vendors

# Load the data globally
data = pd.read_csv('C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/co_traveller/df_common.csv')
data.drop(columns=['Unnamed: 0', 'main_id', 'latitude_grid', 'longitude_grid', 'grid'], inplace=True)
data['date'] = pd.to_datetime(data['usage_timeframe'], unit='ms')

# Function to convert latitude and longitude to Web Mercator coordinates
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
original_source = ColumnDataSource(data)
table_source = ColumnDataSource(data)
map_source = ColumnDataSource(data)

# Color palettes and styles
dow_color_palette = ['#03045E', '#023E8A', '#0077B6', '#0096C7', '#00B4D8', '#48CAE4', '#90E0EF', '#ADE8F4', '#CAF0F8']
months_color_palette = ['#1130A7', '#125F92', '#128F7E', '#13BE69', '#13EE55', '#A4D82F', '#ECCD1C', '#FC9C14', '#CB620F', '#9A270A', '#561463', '#1100BB']

# Main plot
p = figure(height=400, width=1000, tools="xpan", toolbar_location=None,
           x_axis_type="datetime", x_axis_location="below",
           background_fill_color="white", x_range=(agg_data['date'][0], agg_data['date'][len(agg_data) // 2]),
           y_range=(0, agg_data['hits'].max()*1.2), align="start")

p.line('date', 'hits', source=source, legend_label="Hits", color="darkblue", line_width=2)
p.circle('date', 'hits', source=source, fill_color="darkblue", size=8, line_color="darkblue")

p.extra_y_ranges = {"unique_devices": Range1d(start=0, end=agg_data['unique_devices'].max()*1.2)}
p.add_layout(LinearAxis(y_range_name="unique_devices"), 'right')

p.line('date', 'unique_devices', source=source, legend_label="Unique Devices", color="red", y_range_name="unique_devices", line_width=2)
p.circle('date', 'unique_devices', source=source, fill_color="red", size=8, line_color="red", y_range_name="unique_devices")

# Adjust legend location and styling
p.legend.location = "top_left"
p.legend.background_fill_alpha = 0.5
p.legend.label_text_font_size = '8pt' 
p.legend.label_text_font = 'arial'
p.legend.orientation = "vertical"
p.legend.border_line_color = None
p.xgrid.grid_line_color = None
p.ygrid.grid_line_color = None

# Styling axis labels and title
p.yaxis.axis_label = 'Total Hits'
p.right[0].axis_label = 'Active Devices'
p.title = Title(text="Device Hits and Unique Devices", align="left", text_font="Bahnschrift SemiBold", text_font_size="15pt", text_color='#03045E')

# Range selection plot
select = figure(height=100, width=1000, y_range=p.y_range, x_axis_type="datetime", y_axis_type=None, tools="", 
                toolbar_location=None, background_fill_color="white", align="center")
range_tool = RangeTool(x_range=p.x_range)
range_tool.overlay.fill_color = "gray"
range_tool.overlay.fill_alpha = 0.5

select.line('date', 'hits', source=source, color="darkblue")
select.circle('date', 'hits', source=source, fill_color="darkblue", size=8, line_color="darkblue")
select.extra_y_ranges = {"unique_devices": p.extra_y_ranges["unique_devices"]}
select.line('date', 'unique_devices', source=source, color="red", y_range_name="unique_devices")

select.xgrid.grid_line_color = None
select.ygrid.grid_line_color = None
select.circle('date', 'unique_devices', source=source, fill_color="red", size=8, line_color="red", y_range_name="unique_devices")
select.add_tools(range_tool)

# Data table
columns = [TableColumn(field="date", title="Date", formatter=DateFormatter()),
           TableColumn(field="device_id", title="Device ID"),
           TableColumn(field="location_latitude", title="Latitude"),
           TableColumn(field="location_longitude", title="Longitude")]

# Add Title for the Dataframe
data_table_title = Div(text="<h3 style='text-align:left; font-family:Bahnschrift SemiBold; font-size:15pt; color:#03045E; margin:0;'>Detailed Device Data</h3>")

data_table = DataTable(source=table_source, columns=columns, width=900, height=300)
data_table_title = Div(text="<h3 style='text-align:left; font-family:Bahnschrift SemiBold; font-size:15pt; color:#03045E; margin:0;'>Detailed Device Data</h3>")

# Map plot using Bokeh
tile_provider = get_provider(Vendors.OSM)
map_plot = figure(x_axis_type="mercator", y_axis_type="mercator",
                  width=1050, height=600, tools="pan,wheel_zoom,box_zoom,reset", active_scroll="wheel_zoom")
map_plot.add_tile(tile_provider)
map_plot.circle(x='x', y='y', size=10, color="darkblue", fill_alpha=0.4, source=map_source)
map_plot.add_layout(Title(text="Geospatial Data", align="left", text_font="Bahnschrift SemiBold", text_font_size="15pt", text_color='#03045E'), 'above')

# Set initial view for the map to avoid stretching
initial_x_range = (data['x'].min() - 10000, data['x'].max() + 10000)
initial_y_range = (data['y'].min() - 10000, data['y'].max() + 10000)
map_plot.x_range = Range1d(*initial_x_range)
map_plot.y_range = Range1d(*initial_y_range)
map_plot.xaxis.visible = False
map_plot.yaxis.visible = False
map_plot.xgrid.grid_line_color = None
map_plot.ygrid.grid_line_color = None

p.margin = (0, 0, 0, 200)  # Adjust the last number to set the left margin
select.margin = (0, 0, 0, 200)  # Same left margin for consistency
data_table.margin = (0, 0, 0, 220)  # Apply the margin to the data table as well
map_plot.margin = (0, 0, 0, 180)  # Apply the margin to the map plot
data_table_title.margin = (0, 0, 0, 220)


# JavaScript callback to update table and map based on selected range
callback = CustomJS(args=dict(original_source=original_source, table_source=table_source, map_source=map_source), 
                    code="""
    var original_data = original_source.data;
    var table_data = table_source.data;
    var map_data = map_source.data;
    var start = cb_obj.start;
    var end = cb_obj.end;

    table_data['date'] = [];
    table_data['device_id'] = [];
    table_data['location_latitude'] = [];
    table_data['location_longitude'] = [];

    map_data['x'] = [];
    map_data['y'] = [];

    for (var i = 0; i < original_data['date'].length; i++) {
        var date = new Date(original_data['date'][i]).getTime();
        if (date >= start && date <= end) {
            table_data['date'].push(original_data['date'][i]);
            table_data['device_id'].push(original_data['device_id'][i]);
            table_data['location_latitude'].push(original_data['location_latitude'][i]);
            table_data['location_longitude'].push(original_data['location_longitude'][i]);

            map_data['x'].push(original_data['x'][i]);
            map_data['y'].push(original_data['y'][i]);
        }
    }
    table_source.change.emit();
    map_source.change.emit();
""")


select.js_on_change('x_range', callback)
p.x_range.js_on_change('start', callback)
p.x_range.js_on_change('end', callback)

space1 = Div(text="", styles={'height': '20px', 'width': '100%'})
space2 = Div(text="", styles={'height': '20px', 'width': '100%'})

# Combine plots, table, and map
layout = column(p, select, space1, data_table_title, data_table, space2, map_plot)

# Output to HTML file
output_file("interactive_dashboard.html", title="Interactive Dashboard")

show(layout)
