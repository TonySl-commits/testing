##############################################################################################################################
    # Imports
##############################################################################################################################

import pandas as pd
import numpy as np
import folium
import plotly.graph_objects as go
import folium
from datetime import datetime, timedelta, timezone
from folium.plugins import MarkerCluster
import folium
from jinja2 import Template
from folium.map import Marker
from bs4 import BeautifulSoup

from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.timeline.timeline_functions import TimeLineFunctions
from vcis.databases.oracle.oracle_tools import OracleTools

class PlotsTools:
    def __init__(self, verbose=False):
        self.properties = CDR_Properties()
        self.utils = CDR_Utils(verbose=True)
        self.cassandra_tools = CassandraTools()
        self.timeline_functions = TimeLineFunctions()
        self.verbose = verbose
        self.oracle_tools = OracleTools()

##############################################################################################################################
    # Plotting Functions
##############################################################################################################################
   
    def get_device_history_geo_plot(self , df_geo:pd.DataFrame , start_date:str ,end_date:str):

        trace_device = go.Scattermapbox(
            lat=df_geo['location_latitude'],
            lon=df_geo['location_longitude'],
            mode='markers',
            marker=dict(
                size=10,
                color='blue',
                opacity=0.7
            ),
            hovertext=df_geo['device_id'],
            name='Geo_Device'
        )

        # Create the layout for the map
        layout = go.Layout(
            title='Correlation Map Locations',
            mapbox=dict(
                style='open-street-map',
                zoom=10
            ),
            showlegend=True
        )

        fig = go.Figure(data=trace_device, layout=layout)
        fig.write_html(self.properties.correlation_map_path + 'Device_History_plot_map.html')
        print('################ Device History Map Created ################')

    def get_correlation_plot(self , geo_id:str , imsi_id:str , start_date:str ,end_date:str):
        df_geo = self.cassandra_tools.get_device_history_geo(device_id= geo_id ,start_date= start_date ,end_date= end_date)
        df_imsi = self.cassandra_spark_tools.get_device_history_imsi_spark(imsi_id= imsi_id , start_date= start_date , end_date= end_date)

        trace_device = go.Scattermapbox(
            lat=df_geo['location_latitude'],
            lon=df_geo['location_longitude'],
            mode='markers',
            marker=dict(
                size=10,
                color='blue',
                opacity=0.1
            ),
            hovertext=df_geo['device_id'],
            name='Geo_Device'
        )

        # Create a scatter mapbox trace for df_device_co
        trace_cotravler = go.Scattermapbox(
            lat=df_imsi['location_latitude'],
            lon=df_imsi['location_longitude'],
            mode='markers',
            marker=dict(
                size=10,
                color='red',
                opacity=0.7
            ),
            hovertext=df_imsi['imsi_id'],
            name='IMSI_Device'
        )

        # Create the layout for the map
        layout = go.Layout(
            title='Correlation Map Locations',
            mapbox=dict(
                style='open-street-map',
                zoom=10
            ),
            showlegend=True
        )

        fig = go.Figure(data=[trace_device, trace_cotravler], layout=layout)
        fig.write_html(self.properties.correlation_map_path + 'Correlation_map.html')
        print('################ Correlation Map Created ################')

    def get_correlation_plot_folium(self , geo_id:str , imsi_id:str , start_date:str ,end_date:str,server:str = '10.1.10.66'):
        df_geo = self.cassandra_tools.get_device_history_geo(device_id= geo_id ,start_date= start_date ,end_date= end_date,server = server)
        df_imsi = self.cassandra_tools.get_device_history_imsi(imsi_id= imsi_id , start_date= start_date , end_date= end_date,server= server)
        m = folium.Map(location=[df_imsi['location_latitude'][0],df_imsi['location_longitude'][0]], zoom_start=15)
        
        df_geo = self.utils.convert_ms_to_datetime(df_geo)
        df_imsi = self.utils.convert_ms_to_datetime(df_imsi)
        df_imsi = df_imsi.dropna(subset=['location_latitude','location_longitude','location_azimuth'])
        for index , row in df_imsi.iterrows():
            latitude = row['location_latitude']
            longitude = row['location_longitude']
            azimuth=int(row['location_azimuth'])
            popup = row['usage_timeframe']
            triangle_coordinates = self.utils.calculate_sector_triangle(latitude, longitude, azimuth)
            folium.Polygon(locations=triangle_coordinates, color='green', fill=True, fill_color='green', fill_opacity=0.1).add_to(m)
            folium.CircleMarker([latitude, longitude],radius=1,color='red',popup=popup).add_to(m)
            
        for index , row in df_geo.iterrows():
            latitude = row['location_latitude']
            longitude = row['location_longitude']
            popup = row['usage_timeframe']
            folium.CircleMarker([latitude, longitude],radius=1,color='blue',popup=popup).add_to(m)
        time = datetime.now()
        m.save(self.properties.passed_filepath_cdr_map + f'Correlation_map_{time}.html')
        print(f'################ Correlation Map Created ################\nName: Correlation_map_{time}.html')
        print(df_geo.head())
        print(df_imsi.head())
        return m

    def is_within_intervals(self, popup, interval_dfs):
        for interval_df in interval_dfs:
            if interval_df['usage_timeframe'].min() <= popup <= interval_df['usage_timeframe'].max():
                return True
        return False

    def get_correlation_plot_folium_filtered(self, start_date, end_date, interval_dfs, map, imsi_id:str):
        df_imsi = self.cassandra_spark_tools.get_device_history_imsi_spark(imsi_id=imsi_id, start_date=start_date, end_date=end_date)

        print(f"Number of rows in CDR data: {len(df_imsi)}")

        count = 0

        for i in range(len(df_imsi)):
            latitude = df_imsi['location_latitude'][i]
            longitude = df_imsi['location_longitude'][i]
            azimuth = int(df_imsi['location_azimuth'][i])
            popup = df_imsi['usage_timeframe'][i]

            # Check if the popup is in any of the intervals
            in_interval = self.is_within_intervals(popup, interval_dfs)

            if in_interval:
                triangle_coordinates = self.utils.calculate_sector_triangle(latitude, longitude, azimuth)
                folium.Polygon(locations=triangle_coordinates, color='green', fill=True, fill_color='green', fill_opacity=0.1).add_to(map)
                folium.CircleMarker([latitude, longitude], radius=1, color='red', popup=self.utils.convert_ms_to_datetime_value(popup)).add_to(map)
                count += 1

        print(f"Number of rows filtered according to Geospatial data: {count}")

        time = datetime.now()
        map.save(self.properties.passed_filepath_gpx_map + f'Correlation_map_{time}.html')
        print(f'################ Correlation Map Created ################\nName: Correlation_map_{time}.html')

        return map

    def get_cotraveler_plot_folium(self , geo_id:str , geo_id_cotraveler:str , start_date:int ,end_date:int,server: str = '10.10.10.101'):

        df_geo = self.cassandra_tools.get_device_history_geo(device_id= geo_id ,start_date= start_date ,end_date= end_date,server=server)
        df_geo_cotraveler = self.cassandra_tools.get_device_history_geo(device_id= geo_id_cotraveler ,start_date= start_date ,end_date= end_date,server = server)
       
        df_geo = self.utils.convert_ms_to_datetime(df_geo)
        df_geo_cotraveler = self.utils.convert_ms_to_datetime(df_geo_cotraveler)
        
        m = folium.Map(location=[df_geo_cotraveler['location_latitude'].mean(),df_geo_cotraveler['location_longitude'].mean()], zoom_start=7)
        
        for index, location_info in df_geo.iterrows():
            folium.CircleMarker([location_info["location_latitude"], location_info["location_longitude"]],radius=3,color='red',popup=location_info['usage_timeframe']).add_to(m)
        
        for index, location_info in df_geo_cotraveler.iterrows():
            folium.CircleMarker([location_info["location_latitude"], location_info["location_longitude"]],radius=1,color='blue',popup=location_info['usage_timeframe']).add_to(m)
         
        time = datetime.now()
        path = self.properties.passed_filepath_gpx_map + f'Cotraveler_map_{time}.html'
        m.save(path)
        print(f'################ Cotraveler Map Created ################\nName: Ctrmap_{time}.html')
        return path

    def bts_nodes_cgi_plot(self,data:pd.DataFrame):
        m = folium.Map(location=[data['node_latitude'].mean(),data['node_latitude'].mean()], zoom_start=7)
        
        for index, location_info in data.iterrows():
            latitude = location_info['bts_latitude']
            longitude = location_info['bts_longitude']
            azimuth=int(location_info['bts_azimuth'])
            triangle_coordinates = self.utils.calculate_sector_triangle(latitude, longitude, azimuth)
            folium.Polygon(locations=triangle_coordinates, color='green', fill=True, fill_color='green', fill_opacity=0.1).add_to(m)
            folium.CircleMarker([latitude, longitude],radius=1,color='red').add_to(m)
            # folium.CircleMarker([location_info["node_latitude"], location_info["node_longitude"]],radius=1,color='black').add_to(m)
            folium.CircleMarker([location_info["gps_latitude"], location_info["gps_longitude"]],radius=1,color='blue').add_to(m)
       
        time = datetime.now()
        path = self.properties.passed_filepath_gpx_map + f'bts_nodes_map.html'
        m.save(path)
        print(f'################ Map Created ################\nName: bts_nodes_map.html')
        return path

    def get_simulation_plot(self,data:pd.DataFrame=None,shapes:dict=None):
        data['Timestamp'] = str(datetime.fromtimestamp(min(data['usage_timeframe']) / 1000))

        click_template = """{% macro script(this, kwargs) %}
            var {{ this.get_name() }} = L.marker(
                {{ this.location|tojson }},
                {{ this.options|tojson }}
            ).addTo({{ this._parent.get_name() }}).on('click', onClick);
        {% endmacro %}"""
        click_template_cluster = """{% macro script(this, kwargs) %}
            var {{ this.get_name() }} = L.markerClusterGroup(
                {{ this.options|tojson }}
            ).addTo({{ this._parent.get_name() }}).on('clusterclick', onClick_cluster);
            {{ this.get_name() }}.options.zoomToBoundsOnClick = false; 
            {%- if this.icon_create_function is not none %}
            {{ this.get_name() }}.options.iconCreateFunction =
                {{ this.icon_create_function.strip() }};
            {%- endif %}
        {% endmacro %}"""

        Marker._template = Template(click_template)
        MarkerCluster._template = Template(click_template_cluster)

        m = folium.Map(tiles = None, zoom_start=13)
        
        folium.TileLayer(tiles = 'https://tile.openstreetmap.org/{z}/{x}/{y}.png', 
                            attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors', 
                            name='Light',
                            max_zoom = 21).add_to(m)
        
        folium.TileLayer(tiles ='https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', 
                                    attr= 'Dark Mode Tiles © CartoDB',
                                    name='Dark',
                                    max_zoom = 21).add_to(m)
        
        folium.TileLayer(tiles ='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', 
                                    attr= 'Satellite Imagery © Esri',
                                    name='Satellite',
                                    max_zoom = 19).add_to(m)

        click_js = """function onClick(e) { 
            var rowData = [{latitude: e.latlng.lat, longitude: e.latlng.lng, device_id: e.options.options.id,date: e.options.options.date}];

            var columnDefs = [
                { headerName: 'Device ID', field: 'device_id', width: 200, cellStyle: {textAlign: 'center'}, headerStyle: {textAlign: 'center'}},
                { headerName: 'Latitude', field: 'latitude', width: 100, cellStyle: {textAlign: 'center'}, headerStyle: {textAlign: 'center'}},
                { headerName: 'Longitude', field: 'longitude', width: 100, cellStyle: {textAlign: 'center'}, headerStyle: {textAlign: 'center'}},
                { headerName: 'Date', field: 'date', width: 100, cellStyle: {textAlign: 'center'}, headerStyle: {textAlign: 'center'}}
            ];

            var tableHTML = "<div id='agGridTable' style='width: 100%; height: 300px;' class='ag-theme-alpine'></div>";

            // Add popup to marker
            L.popup()
                .setLatLng(e.latlng)
                .setContent(tableHTML)
                .openOn(e.target._map);

            // Initialize ag-Grid in the popup after it is opened
            setTimeout(function() {
                displayAgGrid(rowData, columnDefs);
            }, 100);
        }"""
        click_cluster_js = """function onClick_cluster(e) {
            var cluster = e.layer.getAllChildMarkers();
            var rowData = [];
            for (var i = 0; i < cluster.length; i++) {
                var marker = cluster[i];
                rowData.push({
                    latitude: marker.getLatLng().lat,
                    longitude: marker.getLatLng().lng,
                    device_id: marker.options.options.id,
                    date: marker.options.options.date
                });
            }

            var columnDefs = [
                { headerName: 'Device ID', field: 'device_id', width: 200, cellStyle: {textAlign: 'center'}, headerStyle: {textAlign: 'center'}},
                { headerName: 'Latitude', field: 'latitude', width: 100, cellStyle: {textAlign: 'center'}, headerStyle: {textAlign: 'center'}},
                { headerName: 'Longitude', field: 'longitude', width: 100, cellStyle: {textAlign: 'center'}, headerStyle: {textAlign: 'center'}},
                { headerName: 'Date', field: 'date', width: 100, cellStyle: {textAlign: 'center'}, headerStyle: {textAlign: 'center'}}
            ];

            var tableHTML = "<div id='agGridTable' style='width: 100% !important; height: 300px;' class='ag-theme-alpine'></div>";

            // Add popup to cluster
            L.popup()
                .setLatLng(e.latlng)
                .setContent(tableHTML)
                .addTo(e.target._map)
                .openOn(e.target._map);

            // Initialize ag-Grid in the popup after it is opened
            setTimeout(function() {
                displayAgGrid(rowData, columnDefs);
            }, 100);
        }"""

        e = folium.Element(click_js)
        e_cluster = folium.Element(click_cluster_js)

        html = m.get_root()
        html.script.get_root().render()
        html.script._children[e_cluster.get_name()] = e_cluster
        html.script._children[e.get_name()] = e

        marker_cluster = MarkerCluster(overlay = False,control = False,options=dict(spiderfyOnMaxZoom=False, singleMarkerMode=True)).add_to(m)

        for index, row in data.iterrows():
            Marker(location=(row['location_latitude'], row['location_longitude']),
                options={'id': row['device_id'], 'date': row['Timestamp']}).add_to(marker_cluster)

        aggrid_html = """
        <style>
        .leaflet-popup-content-wrapper {
            width: 520px; 
            height: 400px; 
            overflow-y: scroll;
            border-radius:0 !important;
        }

        .leaflet-popup-content {
            width: 100% !important;
            height: 300px;
            display: flex;
            align-items: center;
            justify-content: center;
            padding-left: 1px;
            padding-right: 1px;
            padding-top: 20px;
            padding-bottom: 20px;
            margin: 0;
        }

        .close {
            color: #aaa;
            position: absolute;
            top: 10px;
            right: 25px;
            font-size: 28px;
            font-weight: bold;
            cursor: pointer;
        }

        .close:hover,

        .close:focus {
            color: black;
            text-decoration: none;
            cursor: pointer;
        }
        </style>

        <script src="https://cdn.jsdelivr.net/npm/ag-grid-community/dist/ag-grid-community.min.noStyle.js"></script>
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ag-grid-community/styles/ag-grid.css">
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ag-grid-community/styles/ag-theme-alpine.css">
        <script>

            function displayAgGrid(rowData, columnDefs) {
                var gridOptions = {
                    columnDefs: columnDefs,
                    rowData: rowData,
                    domLayout: 'autoHeight'
                };

                var eGridDiv = document.querySelector('#agGridTable');
                new agGrid.Grid(eGridDiv, gridOptions);
            }

            document.addEventListener('DOMContentLoaded', (event) => {
                var span = document.getElementsByClassName('close')[0];

                span.onclick = function() {
                    var popup = document.querySelector('.leaflet-popup-content-wrapper');
                    popup.parentNode.removeChild(popup);
                }

                window.onclick = function(event) {
                    var popup = document.querySelector('.leaflet-popup-content-wrapper');
                    if (event.target == popup) {
                        popup.parentNode.removeChild(popup);
                    }
                }
            });
        </script>
        """
        html.html.add_child(folium.Element(aggrid_html))

        for circle in shapes:
            folium.Circle(location=[circle[0]["lat"], circle[0]["lng"]], radius=circle[1], color='cornflowerblue', fill=True, fill_opacity=0.6, opacity=1).add_to(m)
        
        folium.LayerControl(
                position                = "topright",                                   
                title                   = "Open full-screen map",                       
                title_cancel            = "Close full-screen map",                      
                force_separate_button   = True,
                layers=['Dark', 'Satellite']
            ).add_to(m)

        path = self.properties.passed_filepath_cdr_map + f'simulation_plot.html'
        m.save(path)

        print(f'################ Map Created ################\nName: simulation_plot.html')
        return m

