from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import uvicorn

app = FastAPI()

# Load the data globally
data = pd.read_csv('C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/co_traveller/df_common.csv')

data.drop(columns=['Unnamed: 0', 'main_id', 'latitude_grid', 'longitude_grid', 'grid'], inplace=True)
data.rename(columns={'usage_timeframe': 'Date',
                    'location_latitude' : 'Latitude', 
                     'location_longitude': 'Longitude',
                     'device_id': 'Device ID',
                     'location_name': 'Location Name',
                     'service_provider_id': 'Service Provider ID'}, inplace=True)

data['Date'] = pd.to_datetime(data['Date'], unit='ms')

# Aggregate the data for the line chart
agg_data = data.groupby(data['Date'].dt.date).agg(
    hits=('Device ID', 'count'),
    unique_devices=('Device ID', 'nunique')
).reset_index()

# Function to create the line chart with dual y-axes and a slider
def create_line_chart(data):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=data['Date'], y=data['hits'], name='Hits'))
    fig.add_trace(go.Scatter(x=data['Date'], y=data['unique_devices'], name='Unique Devices', yaxis='y2'))

    fig.update_layout(
        yaxis2=dict(
            overlaying='y',
            side='right'
        ),
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(step="all")
                ])
            ),
            rangeslider=dict(visible=True),
            type="date"
        )
    )
    return fig.to_html(full_html=False)

# Function to create a dynamic table
def create_dynamic_table(data, start_date, end_date):
    filtered_data = data[(data['Date'] >= start_date) & (data['Date'] <= end_date)]
    top_rows = filtered_data.head(5)
    bottom_rows = filtered_data.tail(5)
    display_data = pd.concat([top_rows, bottom_rows])
    display_data.reset_index(drop=True, inplace=True)
    return display_data.to_html(index=False)

# Function to create a map
def create_map(data, start_date, end_date):
    filtered_data = data[(data['Date'] >= start_date) & (data['Date'] <= end_date)]
    fig = px.scatter_mapbox(filtered_data, lat="Latitude", lon="Longitude", hover_name="Device ID", zoom=3)
    fig.update_layout(mapbox_style="open-street-map")
    return fig.to_html(full_html=False)

@app.get("/", response_class=HTMLResponse)
async def index():
    start_date = pd.to_datetime(agg_data['Date'].min())
    end_date = pd.to_datetime(agg_data['Date'].max())

    line_chart_html = create_line_chart(agg_data)
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
    </head>
    <body>
        <h1>Geospatial Data Report</h1>
        <div>
            <h2>Timeline Chart</h2>
            {line_chart_html}
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
        <script>
            document.addEventListener('DOMContentLoaded', function() {{
                var chart = document.querySelector('.plotly-graph-div');

                chart.on('plotly_relayout', function(eventdata) {{
                    if(eventdata['xaxis.range[0]'] && eventdata['xaxis.range[1]']) {{
                        var start_date = new Date(eventdata['xaxis.range[0]']).toISOString();
                        var end_date = new Date(eventdata['xaxis.range[1]']).toISOString();
                        updateTable(start_date, end_date);
                        updateMap(start_date, end_date);
                    }}
                }});

                function updateTable(start_date, end_date) {{
                    fetch(`/update_table?start_date=${{start_date}}&end_date=${{end_date}}`)
                    .then(response => response.text())
                    .then(html => {{
                        document.getElementById('table-container').innerHTML = html;
                    }});
                }}

                function updateMap(start_date, end_date) {{
                    fetch(`/update_map?start_date=${{start_date}}&end_date=${{end_date}}`)
                    .then(response => response.text())
                    .then(html => {{
                        document.getElementById('map-container').innerHTML = html;
                    }});
                }}
            }});
        </script>
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
    uvicorn.run(app, host='10.1.8.55', port=2002, log_level='debug')
