from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from bokeh.plotting import figure
from bokeh.embed import components
from bokeh.models import ColumnDataSource, DateRangeSlider, HoverTool, Range1d, LinearAxis, CustomJS
from bokeh.layouts import column
from bokeh.resources import CDN
import uvicorn
from datetime import datetime

app = FastAPI()

# Load the data globally
data = pd.read_csv('C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/co_traveller/df_common.csv')
data['date'] = pd.to_datetime(data['usage_timeframe'], unit='ms')

# Aggregate the data for the line chart
agg_data = data.groupby(data['date'].dt.date).agg(
    hits=('device_id', 'count'),
    unique_devices=('device_id', 'nunique')
).reset_index()

# Convert `date` column to datetime in agg_data for compatibility
agg_data['date'] = pd.to_datetime(agg_data['date'])

# Function to create the line chart with dual y-axes and a slider
def create_line_chart(data, start_date, end_date):
    filtered_data = data[(data['date'] >= start_date) & (data['date'] <= end_date)]
    source = ColumnDataSource(filtered_data)

    p = figure(x_axis_type="datetime", title="Hits and Unique Devices Over Time", height=350, width=800)
    
    # Primary y-axis
    p.line('date', 'hits', source=source, legend_label="Hits", color="blue")

    # Secondary y-axis
    p.extra_y_ranges = {"unique_devices": Range1d(start=0, end=filtered_data['unique_devices'].max())}
    p.add_layout(LinearAxis(y_range_name="unique_devices", axis_label="Unique Devices"), 'right')
    p.line('date', 'unique_devices', source=source, legend_label="Unique Devices", color="green", y_range_name="unique_devices")

    p.yaxis.axis_label = "Hits"
    
    p.add_tools(HoverTool(
        tooltips=[("Date", "@date{%F}"), ("Hits", "@hits"), ("Unique Devices", "@unique_devices")],
        formatters={'@date': 'datetime'},
        mode='vline'
    ))

    date_range_slider = DateRangeSlider(value=(start_date, end_date), start=start_date, end=end_date, step=1, title="Date Range")
    callback = CustomJS(args=dict(source=source, original_data=filtered_data), code="""
        const data = source.data;
        const start = new Date(cb_obj.value[0]);
        const end = new Date(cb_obj.value[1]);
        for (let i = 0; i < data.date.length; i++) {
            if (new Date(data.date[i]) < start || new Date(data.date[i]) > end) {
                data.hits[i] = null;
                data.unique_devices[i] = null;
            } else {
                data.hits[i] = original_data.hits[i];
                data.unique_devices[i] = original_data.unique_devices[i];
            }
        }
        source.change.emit();
    """)
    date_range_slider.js_on_change("value", callback)

    layout = column(p, date_range_slider)
    script, div = components(layout, CDN)
    return script, div

# Function to create a dynamic table
def create_dynamic_table(data, start_date, end_date):
    filtered_data = data[(data['date'] >= start_date) & (data['date'] <= end_date)]
    top_rows = filtered_data.head(5)
    bottom_rows = filtered_data.tail(5)
    display_data = pd.concat([top_rows, bottom_rows])
    return display_data.to_html(index=False)

# Function to create a map
def create_map(data, start_date, end_date):
    filtered_data = data[(data['date'] >= start_date) & (data['date'] <= end_date)]
    fig = px.scatter_mapbox(filtered_data, lat="location_latitude", lon="location_longitude", hover_name="main_id", zoom=3)
    fig.update_layout(mapbox_style="open-street-map")
    return fig.to_html(full_html=False)

@app.get("/", response_class=HTMLResponse)
async def index():
    start_date = pd.to_datetime(agg_data['date'].min())
    end_date = pd.to_datetime(agg_data['date'].max())

    script, div = create_line_chart(agg_data, start_date, end_date)
    table_html = create_dynamic_table(data, start_date, end_date)
    map_html = create_map(data, start_date, end_date)

    html_template = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Geospatial Data Report</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            table {{
                border-collapse: collapse;
                width: 100%;
            }}
            th, td {{
                text-align: left;
                padding: 8px;
            }}
            tr:nth-child(even) {{
                background-color: #f2f2f2;
            }}
        </style>
        {script}
    </head>
    <body>
        <h1>Geospatial Data Report</h1>
        <div>
            <h2>Timeline Chart</h2>
            {div}
        </div>
        <div>
            <h2>Data Table</h2>
            <div id="table-container">
                {table_html}
            </div>
        </div>
        <div>
            <h2>Geospatial Map</h2>
            <div id="map-container">
                {map_html}
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_template, status_code=200)

@app.get("/update_table", response_class=HTMLResponse)
async def update_table(request: Request):
    start_date = datetime.fromisoformat(request.query_params.get('start_date').replace('Z', '+00:00'))
    end_date = datetime.fromisoformat(request.query_params.get('end_date').replace('Z', '+00:00'))
    table_html = create_dynamic_table(data, start_date, end_date)
    return HTMLResponse(content=table_html, status_code=200)

@app.get("/update_map", response_class=HTMLResponse)
async def update_map(request: Request):
    start_date = datetime.fromisoformat(request.query_params.get('start_date').replace('Z', '+00:00'))
    end_date = datetime.fromisoformat(request.query_params.get('end_date').replace('Z', '+00:00'))
    map_html = create_map(data, start_date, end_date)
    return HTMLResponse(content=map_html, status_code=200)

if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000, log_level='debug')
