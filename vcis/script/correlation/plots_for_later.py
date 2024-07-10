def step_one(self,geo_id):
    import pandas as pd
    import time
    import folium
    import math
    from vcis.utils.utils import CDR_Utils
    from vcis.utils.properties import CDR_Properties
    from vcis.databases.cassandra.cassandra_tools import CassandraTools
    from vcis.nodes.nodes_functions import Nodes
    from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
    from vcis.plots.plots_tools import PlotsTools
    import plotly.graph_objects as go
    import plotly.express as px
    from math import radians, cos, sin, asin, sqrt, pi
    import math
    import math

    def calculate_distance(x1, y1, x2, y2):
        return math.hypot(x2 - x1, y2 - y1)
    utils = CDR_Utils()
    properties = CDR_Properties()
    cassandra_tools = CassandraTools(verbose=True)
    nodes_functions = Nodes(verbose=True)


    # # Specify Start and End date for Geospatial data
    start_date = "2023-03-01"
    end_date = "2024-01-30"
    geo_id = geo_id
    imsi_id = "121415223435890"
    server = "10.1.10.110"
    distance=30
    binning=30
    scan_distance = int(distance + binning*math.sqrt(2))
    local = None

    # df_imsi = cassandra_spark_tools.get_device_history_imsi_spark(imsi_id= imsi_id , start_date= start_date , end_date= end_date,local =local)
    # df_imsi = utils.convert_ms_to_datetime(df_imsi)

    start_date = utils.convert_datetime_to_ms_str(start_date)
    end_date = utils.convert_datetime_to_ms_str(start_date)
    # # Get Geospatial and CDR data from Cassandra
    df_geo = cassandra_tools.get_device_history_geo(device_id = geo_id , start_date= start_date , end_date= end_date , server=server)
    df_geo = df_geo.head(100)
    df_geo = utils.binning(df_geo,binning)
    df_geo['latitude_grid'] = df_geo['latitude_grid'].round(6)
    df_geo['longitude_grid'] = df_geo['longitude_grid'].round(6)
    df_geo = utils.combine_coordinates(df_geo)
    df_near_nodes = nodes_functions.get_close_nodes(data=df_geo,distance= distance,binning = binning,passed_server=server)
    #######################
    # STEP ONE
    #######################

    for scan_id in range(df_near_nodes['scan_id'].min(),df_near_nodes['scan_id'].max(),1):
        selected_point = df_near_nodes.loc[df_near_nodes['scan_id'] ==  scan_id]
        if selected_point.empty:
            continue
        gps_trace = go.Scattermapbox(
            lat=selected_point['gps_latitude'],
            lon=selected_point['gps_longitude'],
            mode='markers',
            marker=dict(size=10),
            name='GPS Points',
            legendgroup='group1',
            visible='legendonly'
        )

        # Create traces for Nodes
        node_trace = go.Scattermapbox(
            lat=selected_point['node_latitude'],
            lon=selected_point['node_longitude'],
            mode='markers',
            marker=dict(size=6),
            name='Nodes',
            legendgroup='group1',
            visible='legendonly'
        )
        bins_trace = go.Scattermapbox(
        lat=selected_point['latitude_grid'],
        lon=selected_point['longitude_grid'],
        mode='markers',
        marker=dict(size=6),
        name='BINS',
        legendgroup='group1',
        visible='legendonly'
    )
        print("hi")    
    # Diagonal length in meters
        center_lat = selected_point['latitude_grid'].iloc[0]
        center_lon = selected_point['longitude_grid'].iloc[0]
        # Convert meters to degrees (approximately, this will vary depending on location)
        side = binning
        # Convert meters to degrees (approximately, this will vary depending on location)
        meters_per_degree_lat =   111139.0  # Approximate conversion factor
        meters_per_degree_lon =   111139.0 * math.cos(math.radians(center_lat))  # Account for longitude conversion factor varying with latitude

        # Calculate the latitude and longitude differences
        delta_lat = side / meters_per_degree_lat
        delta_lon = side / meters_per_degree_lon

        # Create the coordinates for the square corners
        square_coords = {
            'lat': [center_lat - delta_lat, center_lat + delta_lat, center_lat + delta_lat, center_lat - delta_lat, center_lat - delta_lat],
            'lon': [center_lon - delta_lon, center_lon - delta_lon, center_lon + delta_lon, center_lon + delta_lon, center_lon - delta_lon]
        }
        # Create a Scattermapbox trace for the square
        bin = go.Scattermapbox(
            lat=square_coords['lat'],
            lon=square_coords['lon'],
            mode='lines',
            line=dict(width=2),
            fill='toself',
            fillcolor='rgba(255,  0,  0,  0.5)',  # Semi-transparent red
        )
        # Parameters
        N =  360  # Number of discrete sample points to be generated along the circle

        # Generate points
        circle_lats, circle_lons = [], []
        for k in range(N):
            angle = pi*2*k/N
            dx = scan_distance*cos(angle)
            dy = scan_distance*sin(angle)
            circle_lats.append(center_lat + (180/pi)*(dy/6378137))
            circle_lons.append(center_lon + (180/pi)*(dx/6378137)/cos(center_lat*pi/180))
        circle_lats.append(circle_lats[0])
        circle_lons.append(circle_lons[0])

        # Create a Scattermapbox trace for the circle
        circle = go.Scattermapbox(
            lat=circle_lats,
            lon=circle_lons,
            mode='lines',
            line=dict(width=2),
            fill='toself',
            fillcolor='rgba(0,  0,  255,  0.5)'  # Semi-transparent blue
        )
        layout = go.Layout(
            autosize=True,
            hovermode='closest',
            mapbox=dict(
                bearing=0,
                center=dict(lat=selected_point['latitude_grid'].iloc[0], lon=selected_point['longitude_grid'].iloc[0]),  # Center of the map
                pitch=0,
                zoom=10
            ),
            showlegend=True,
            legend=dict(
                x=0,
                y=1,
                traceorder='normal',
                font=dict(family='sans-serif', size=12, color='black'),
                bgcolor='rgba(255,  255,  255,  0.8)',
                bordercolor='White',
                borderwidth=1
            )
        )

        # Add traces to the figure
        fig = go.Figure(data=[gps_trace, node_trace,bins_trace,bin,circle], layout=layout)

        # Update the layout to use OpenStreetMap
        fig.update_layout(mapbox_style="open-street-map")
        
        fig.write_html(properties.passed_filepath_data + "step_one/" +f"map{scan_id}.html")


    #########################
    # FOR GRIDS
    #########################
    selected_point = df_near_nodes.copy()
    gps_trace = go.Scattermapbox(
        lat=selected_point['latitude_grid'],
        lon=selected_point['longitude_grid'],
        mode='markers',
        marker=dict(size=10),
        name='GPS Points',
        legendgroup='group1',
        visible='legendonly'
    )

    layout = go.Layout(
        autosize=True,
        hovermode='closest',
        mapbox=dict(
            bearing=0,
            center=dict(lat=selected_point['latitude_grid'].iloc[0], lon=selected_point['longitude_grid'].iloc[0]),  # Center of the map
            pitch=0,
            zoom=10
        ),
        showlegend=True,
        legend=dict(
            x=0,
            y=1,
            traceorder='normal',
            font=dict(family='sans-serif', size=12, color='black'),
            bgcolor='rgba(255,  255,  255,  0.8)',
            bordercolor='White',
            borderwidth=1
        )
    )

    # Add traces to the figure
    fig = go.Figure(data=[gps_trace], layout=layout)
    fig.update_layout(mapbox_style="open-street-map")
    fig.write_html(properties.passed_filepath_data + "test/" +f"grids.html")

#######################
# STEP TWO
#######################
    
def step_two():
    print("UNDER CONSTRUCTION >>>>")