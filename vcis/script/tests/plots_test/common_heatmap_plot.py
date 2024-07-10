
import pandas as pd
import plotly.graph_objects as go
import folium
from vcis.utils.utils import CDR_Utils, CDR_Properties
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
from folium.plugins import HeatMap


##########################################################################################################

utils = CDR_Utils(verbose=True)
properties = CDR_Properties()

df_merged_list = pd.read_csv(properties.passed_filepath_excel+'df_merged_devices.csv')
df_main = pd.read_csv(properties.passed_filepath_excel+'df_main.csv')
df_history = pd.read_csv(properties.passed_filepath_excel+'df_history.csv')
df_common = pd.read_csv(properties.passed_filepath_excel+'df_common.csv')
df_common = pd.read_csv(properties.passed_filepath_excel +'df_common.csv')

pd.set_option('display.max_columns', None)
df_common = utils.convert_datetime_to_ms(df_common)
df_common = df_common.drop_duplicates(subset=['device_id','usage_timeframe'])
df_common = df_common[['device_id', 'location_latitude', 'location_longitude','usage_timeframe','grid']]
df_history = df_history[['device_id', 'location_latitude', 'location_longitude','usage_timeframe','grid']]

x=0
for device_id in df_history['device_id'].unique():
    df_device = df_history[df_history['device_id'] == device_id]
    df_device_common = df_common[df_common['device_id'] ==device_id]
    if x==32:
        break
    x=x+1



# df_device_common =  df_common[df_common['device_id'] == '4e79560f-e59a-4d7b-8b91-6dddbd571c57']

fig = go.Figure()

grid_counts = df_device_common.groupby(['grid']).size().reset_index(name='count')
grid_counts['latitude_grid'],grid_counts['longitude_grid']= grid_counts['grid'].str.split(',').str

df_heatmap = df_device_common.groupby(['location_latitude','location_longitude']).size().reset_index(name='count')

# Create a map centered around the mean latitude and longitude of the main device
m = folium.Map(location=[df_main['location_latitude'].mean(), df_main['location_longitude'].mean()], zoom_start=6)
from folium.plugins import MarkerCluster

# Add markers for the main device
marker_cluster_main = MarkerCluster().add_to(m)
for index, row in df_main.iterrows():
    folium.Marker(
        location=[row['location_latitude'], row['location_longitude']],
        popup='Main Device',
        icon=folium.Icon(color='black')
    ).add_to(marker_cluster_main)

# Add markers for the cotraveler device
marker_cluster_cotraveler = MarkerCluster().add_to(m)
for index, row in df_device_common.iterrows():
    folium.Marker(
        location=[row['location_latitude'], row['location_longitude']],
        popup='Cotraveler Device',
        icon=folium.Icon(color='blue')
    ).add_to(marker_cluster_cotraveler)

# Define the color gradient
color_gradient = {
    0: 'green',
    0.5: 'yellow',
    1: 'red'
}

# Add a heatmap for common hits
heat_data = [[row['location_latitude'], row['location_longitude'], row['count']] for index, row in df_heatmap.iterrows()]
heatmap = HeatMap(heat_data, radius=20, gradient=color_gradient)
heatmap.add_to(m)

# Add a custom legend
legend_html = """
<div style="position: fixed; bottom: 50px; left: 50px; width: 150px; height: 75px; background-color: white; z-index:9999; font-size:14px;">
    <p><span style="background-color: black; width: 20px; height: 20px; display: inline-block;"></span> Main Device</p>
    <p><span style="background-color: blue; width: 20px; height: 20px; display: inline-block;"></span> Cotraveler</p>
</div>
"""

m.get_root().html.add_child(folium.Element(legend_html))

# Add layer control
folium.LayerControl().add_to(m)
def create_popup_table(row):
    return folium.Popup(
        f"""
        <table>
            <tr><th>Latitude</th><td>{row['latitude_grid']}</td></tr>
            <tr><th>Longitude</th><td>{row['longitude_grid']}</td></tr>
            <tr><th>Grid</th><td>{row['grid']}</td></tr>
            <tr><th>Count</th><td>{row['count']}</td></tr>
        </table>
        """,
        max_width=250
    )


for index, row in grid_counts.iterrows():
    folium.Circle(
        location=[row['latitude_grid'], row['longitude_grid']],
        radius=50, # Adjust the radius as needed
        color='blue',
        fill=True,
        fill_color='blue',
        fill_opacity=0,
        opacity=0,
        popup=create_popup_table(row)
    ).add_to(m)


m.show_in_browser()



# click_template = """{% macro script(this, kwargs) %}
#     var {{ this.get_name() }} = L.marker(
#         {{ this.location|tojson }},
#         {{ this.options|tojson }}
#     ).addTo({{ this._parent.get_name() }}).on('click', onClick);
# {% endmacro %}"""
# click_template_cluster = """{% macro script(this, kwargs) %}
#     var {{ this.get_name() }} = L.markerClusterGroup(
#         {{ this.options|tojson }}
#     ).addTo({{ this._parent.get_name() }}).on('clusterclick', onClick_cluster);
#     {{ this.get_name() }}.options.zoomToBoundsOnClick = false; 
#     {%- if this.icon_create_function is not none %}
#     {{ this.get_name() }}.options.iconCreateFunction =
#         {{ this.icon_create_function.strip() }};
#     {%- endif %}
# {% endmacro %}"""

# Marker._template = Template(click_template)
# MarkerCluster._template = Template(click_template_cluster)

# m = folium.Map(tiles = None, zoom_start=13)

# folium.TileLayer(tiles = 'https://tile.openstreetmap.org/{z}/{x}/{y}.png', 
#                     attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors', 
#                     name='Light',
#                     max_zoom = 21).add_to(m)

# folium.TileLayer(tiles ='https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', 
#                             attr= 'Dark Mode Tiles © CartoDB',
#                             name='Dark',
#                             max_zoom = 21).add_to(m)

# folium.TileLayer(tiles ='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', 
#                             attr= 'Satellite Imagery © Esri',
#                             name='Satellite',
#                             max_zoom = 19).add_to(m)

# click_js = """function onClick(e) { 
#     var rowData = [{latitude: e.latlng.lat, longitude: e.latlng.lng, device_id: e.options.options.id,date: e.options.options.date}];

#     var columnDefs = [
#         { headerName: 'Device ID', field: 'device_id', width: 200, cellStyle: {textAlign: 'center'}, headerStyle: {textAlign: 'center'}},
#         { headerName: 'Latitude', field: 'latitude', width: 100, cellStyle: {textAlign: 'center'}, headerStyle: {textAlign: 'center'}},
#         { headerName: 'Longitude', field: 'longitude', width: 100, cellStyle: {textAlign: 'center'}, headerStyle: {textAlign: 'center'}},
#         { headerName: 'Date', field: 'date', width: 100, cellStyle: {textAlign: 'center'}, headerStyle: {textAlign: 'center'}}
#     ];

#     var tableHTML = "<div id='agGridTable' style='width: 100%; height: 300px;' class='ag-theme-alpine'></div>";

#     // Add popup to marker
#     L.popup()
#         .setLatLng(e.latlng)
#         .setContent(tableHTML)
#         .openOn(e.target._map);

#     // Initialize ag-Grid in the popup after it is opened
#     setTimeout(function() {
#         displayAgGrid(rowData, columnDefs);
#     }, 100);
# }"""
# click_cluster_js = """function onClick_cluster(e) {
#     var cluster = e.layer.getAllChildMarkers();
#     var rowData = [];
#     for (var i = 0; i < cluster.length; i++) {
#         var marker = cluster[i];
#         rowData.push({
#             latitude: marker.getLatLng().lat,
#             longitude: marker.getLatLng().lng,
#             device_id: marker.options.options.id,
#             date: marker.options.options.date
#         });
#     }

#     var columnDefs = [
#         { headerName: 'Device ID', field: 'device_id', width: 200, cellStyle: {textAlign: 'center'}, headerStyle: {textAlign: 'center'}},
#         { headerName: 'Latitude', field: 'latitude', width: 100, cellStyle: {textAlign: 'center'}, headerStyle: {textAlign: 'center'}},
#         { headerName: 'Longitude', field: 'longitude', width: 100, cellStyle: {textAlign: 'center'}, headerStyle: {textAlign: 'center'}},
#         { headerName: 'Date', field: 'date', width: 100, cellStyle: {textAlign: 'center'}, headerStyle: {textAlign: 'center'}}
#     ];

#     var tableHTML = "<div id='agGridTable' style='width: 100% !important; height: 300px;' class='ag-theme-alpine'></div>";

#     // Add popup to cluster
#     L.popup()
#         .setLatLng(e.latlng)
#         .setContent(tableHTML)
#         .addTo(e.target._map)
#         .openOn(e.target._map);

#     // Initialize ag-Grid in the popup after it is opened
#     setTimeout(function() {
#         displayAgGrid(rowData, columnDefs);
#     }, 100);
# }"""

# e = folium.Element(click_js)
# e_cluster = folium.Element(click_cluster_js)

# html = m.get_root()
# html.script.get_root().render()
# html.script._children[e_cluster.get_name()] = e_cluster
# html.script._children[e.get_name()] = e

# marker_cluster = MarkerCluster(overlay = False,control = False,options=dict(spiderfyOnMaxZoom=False, singleMarkerMode=True)).add_to(m)

# for index, row in data.iterrows():
#     Marker(location=(row['location_latitude'], row['location_longitude']),
#         options={'id': row['device_id'], 'date': row['Timestamp']}).add_to(marker_cluster)

# aggrid_html = """
# <style>
# .leaflet-popup-content-wrapper {
#     width: 520px; 
#     height: 400px; 
#     overflow-y: scroll;
#     border-radius:0 !important;
# }

# .leaflet-popup-content {
#     width: 100% !important;
#     height: 300px;
#     display: flex;
#     align-items: center;
#     justify-content: center;
#     padding-left: 1px;
#     padding-right: 1px;
#     padding-top: 20px;
#     padding-bottom: 20px;
#     margin: 0;
# }

# .close {
#     color: #aaa;
#     position: absolute;
#     top: 10px;
#     right: 25px;
#     font-size: 28px;
#     font-weight: bold;
#     cursor: pointer;
# }

# .close:hover,

# .close:focus {
#     color: black;
#     text-decoration: none;
#     cursor: pointer;
# }
# </style>

# <script src="https://cdn.jsdelivr.net/npm/ag-grid-community/dist/ag-grid-community.min.noStyle.js"></script>
# <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ag-grid-community/styles/ag-grid.css">
# <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ag-grid-community/styles/ag-theme-alpine.css">
# <script>

#     function displayAgGrid(rowData, columnDefs) {
#         var gridOptions = {
#             columnDefs: columnDefs,
#             rowData: rowData,
#             domLayout: 'autoHeight'
#         };

#         var eGridDiv = document.querySelector('#agGridTable');
#         new agGrid.Grid(eGridDiv, gridOptions);
#     }

#     document.addEventListener('DOMContentLoaded', (event) => {
#         var span = document.getElementsByClassName('close')[0];

#         span.onclick = function() {
#             var popup = document.querySelector('.leaflet-popup-content-wrapper');
#             popup.parentNode.removeChild(popup);
#         }

#         window.onclick = function(event) {
#             var popup = document.querySelector('.leaflet-popup-content-wrapper');
#             if (event.target == popup) {
#                 popup.parentNode.removeChild(popup);
#             }
#         }
#     });
# </script>
# """
# html.html.add_child(folium.Element(aggrid_html))
