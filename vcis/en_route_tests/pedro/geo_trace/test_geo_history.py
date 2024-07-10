import pandas as pd
import folium 
import pickle
import osmnx as ox
import networkx as nx

from collections import defaultdict
from cdr_trace.reporting.geo_reporting.aoi_report.aoi_report_functions import AOIReportFunctions

def get_geo_history(geolocation_analyzer):

    all_trajectories_geo_history = []

    for device in range(geolocation_analyzer.geolocation_data_list.get_length()):
        geolocation_data = geolocation_analyzer.geolocation_data_list[device]

        aoi_report = AOIReportFunctions()
        aoi_report.initialize_aoi_report(geolocation_data)
        location_df = aoi_report.get_locations_df()

        location_df['Location'] = location_df['Location'].astype(str)
        location_df['Timestamp'] = pd.to_datetime(location_df['Timestamp'])
        location_df.sort_values(by='Timestamp', inplace=True)
        location_df.reset_index(drop=True, inplace=True)

        previous_aoi = None
        current_trajectory = []

        for i, row in location_df.iterrows():
            current_location = row['Location']
            # Look ahead to find the start of the next AOI if the current location is not "Other"
            if i < len(location_df) - 1 and current_location != "Other":
                next_row = location_df.iloc[i + 1]
                if next_row['Location'] != "Other" and next_row['Location'] != current_location:
                    next_aoi_start = next_row['Location']
                elif next_row['Location'] == "Other":
                    # Look further ahead beyond "Other" to find the next AOI
                    for j in range(i + 2, len(location_df)):
                        further_row = location_df.iloc[j]
                        if further_row['Location'] != "Other":
                            next_aoi_start = further_row['Location']
                            break

            if current_location != "Other":
                if current_location != previous_aoi and previous_aoi is not None:
                    if current_trajectory:
                        end_AOI = next_aoi_start if next_aoi_start else current_location
                        all_trajectories_geo_history.append((pd.DataFrame(current_trajectory), geolocation_data.device_id_geo, previous_aoi, end_AOI))
                        current_trajectory = []
                        next_aoi_start = None  # Reset for next use

                previous_aoi = current_location
                current_trajectory.append(row.to_dict())
            else:
                if current_trajectory:
                    current_trajectory.append(row.to_dict())

        # Handle the last trajectory if not saved
        if current_trajectory and previous_aoi is not None:
            end_AOI = next_aoi_start if next_aoi_start else previous_aoi
            all_trajectories_geo_history.append((pd.DataFrame(current_trajectory), geolocation_data.device_id_geo, previous_aoi, end_AOI))

    # Now group by AOI pairs
    aoi_pairs_trajectories = group_trajectories_by_aoi_pairs(all_trajectories_geo_history)

    return aoi_pairs_trajectories

def group_trajectories_by_aoi_pairs(all_trajectories):
    aoi_pairs_trajectories = defaultdict(list)

    for trajectory, device_id, start_AOI, end_AOI in all_trajectories:
        aoi_pairs_trajectories[(start_AOI, end_AOI)].append((trajectory, device_id))

    return dict(aoi_pairs_trajectories)

############################################################################################################
def get_maps_history_geo_history(geolocation_analyzer):
    all_trajectories_with_device_id = []

    for device in range(geolocation_analyzer.geolocation_data_list.get_length()):
        geolocation_data = geolocation_analyzer.geolocation_data_list[device]

        aoi_report = AOIReportFunctions()
        aoi_report.initialize_aoi_report(geolocation_data)
        location_df = aoi_report.get_locations_df()

        location_df['Location'] = location_df['Location'].astype(str)
        location_df['Timestamp'] = pd.to_datetime(location_df['Timestamp'])
        location_df.sort_values(by='Timestamp', inplace=True)
        location_df.reset_index(drop=True, inplace=True)

        previous_aoi = None
        current_trajectory = []

        for _, row in location_df.iterrows():
            current_location = row['Location']
            if current_location != "Other":
                if current_location != previous_aoi and previous_aoi is not None:
                    # Save the current trajectory if it's a valid transition (AOI to a different AOI)
                    if current_trajectory:
                        all_trajectories_with_device_id.append((pd.DataFrame(current_trajectory), geolocation_data.device_id_geo))
                        current_trajectory = []

                # Reset or start new trajectory
                previous_aoi = current_location
                current_trajectory.append(row)
            else:
                # Include "Other" locations if we are within a trajectory
                if current_trajectory:
                    current_trajectory.append(row)
        
        # Handle the last trajectory if not saved
        if current_trajectory and previous_aoi is not None:
            all_trajectories_with_device_id.append((pd.DataFrame(current_trajectory), geolocation_data.device_id_geo))

    # Pass collected trajectories to the mapping function
    create_map_history_geo_with_path(all_trajectories_with_device_id)  
    # create_map_history_geo(all_trajectories_with_device_id)

############################################################################################################
    
# PLOT MAPS WITHOUT SHORTEST PATH
def create_map_history_geo(all_trajectories_with_device_id):
    # Iterate over each trajectory and its corresponding device ID
    for index, (trajectory_df, device_id) in enumerate(all_trajectories_with_device_id):
        # Assuming the DataFrame is not empty and has the required columns
        if not trajectory_df.empty:
            # Initialize the map centered on the first point of the trajectory
            initial_location = [trajectory_df.iloc[0]['Latitude'], trajectory_df.iloc[0]['Longitude']]
            m = folium.Map(location=initial_location, zoom_start=12)
            
            # Plot the first point with a special marker (e.g., green)
            first_point = trajectory_df.iloc[0]

            folium.CircleMarker(
                location=[first_point['Latitude'], first_point['Longitude']],
                radius=50,
                color='darkblue',
                fill=True,
                fill_color='blue',
                fill_opacity=0.3,
                tooltip=f"COORDINATES: ({first_point['Latitude']}, {first_point['Longitude']})<br>Timestamp: {first_point['Timestamp']}"
            ).add_to(m)
            
            # Plot intermediate points with blue markers
            for _, point in trajectory_df.iloc[1:-1].iterrows():
                folium.Marker(
                    location=[point['Latitude'], point['Longitude']],
                    popup=f"Time: {point['Timestamp']}",
                    icon=folium.Icon(color='blue', icon='info-sign')
                ).add_to(m)
            
            # Plot the last point with a special marker (e.g., red)
            last_point = trajectory_df.iloc[-1]
            folium.Marker(
                location=[last_point['Latitude'], last_point['Longitude']],
                popup=f"End: {last_point['Timestamp']}",
                icon=folium.Icon(color='red', icon='stop')
            ).add_to(m)
            
            # Save the map to an HTML file
            map_filename = f"trajectory_{index+1}_device_{device_id}.html"
            map_path = f"/u01/jupyter-scripts/Pedro/CDR_Trace_N/CDR_Trace/data/maps/history_geo/{map_filename}"
            m.save(map_path)
            print(f"Map saved to: {map_path}")

############################################################################################################

# PLOT MAPS WITH SHORTEST PATH
def create_map_history_geo_with_path(all_trajectories_with_device_id):
    for index, (trajectory_df, device_id) in enumerate(all_trajectories_with_device_id):
        if not trajectory_df.empty:
            # Initialize the map
            initial_location = [trajectory_df.iloc[0]['Latitude'], trajectory_df.iloc[0]['Longitude']]
            m = folium.Map(location=initial_location, zoom_start=15)
            
            print("Started G osmx")
            # Retrieve the geospatial graph for the entire trajectory
            latitudes = trajectory_df['Latitude']
            longitudes = trajectory_df['Longitude']
            G = get_geospatial_graph((latitudes.min(), longitudes.min()), (latitudes.max(), longitudes.max()))


            # Plot the shortest path between each consecutive marker
            for i in range(len(trajectory_df) - 1):
                start_point = (trajectory_df.iloc[i]['Latitude'], trajectory_df.iloc[i]['Longitude'])
                end_point = (trajectory_df.iloc[i + 1]['Latitude'], trajectory_df.iloc[i + 1]['Longitude'])
                
                # Find the nearest graph nodes to the start and end points
                start_node = find_nearest_graph_node(G, start_point)
                end_node = find_nearest_graph_node(G, end_point)
                
                # Find the best path between these nodes
                best_path = find_best_path(G, start_node, end_node)
                
                # Convert the nodes in the path back to coordinates and plot on the map
                path_coords = [convert_node_to_coords(G, node) for node in best_path]
                folium.PolyLine(path_coords, color="blue", weight=2.5, opacity=1).add_to(m)

            print("Ended G osmx")

            # Add markers for the first and last points with special icons
            m = add_special_markers(m, trajectory_df)

            # Save the map
            map_filename = f"trajectory_{index+1}_device_{device_id}.html"
            map_path = f"/u01/jupyter-scripts/Pedro/CDR_Trace_N/CDR_Trace/data/maps/history_geo/{map_filename}"
            m.save(map_path)
        
            print(f"Map saved to: {map_path}")

def add_special_markers(m, trajectory_df):
    # Add special markers for the first and last points in the trajectory

    # Plot the first point with a special marker (e.g., green)
    first_point = trajectory_df.iloc[0]

    folium.CircleMarker(
        location=[first_point['Latitude'], first_point['Longitude']],
        radius=50,
        color='darkblue',
        fill=True,
        fill_color='blue',
        fill_opacity=0.3,
        tooltip=f"COORDINATES: ({first_point['Latitude']}, {first_point['Longitude']})<br>Timestamp: {first_point['Timestamp']}"
    ).add_to(m)

    # Plot intermediate points with blue markers
    for _, point in trajectory_df.iloc[1:-1].iterrows():
        folium.Marker(
            location=[point['Latitude'], point['Longitude']],
            popup=f"Time: {point['Timestamp']}",
            icon=folium.Icon(color='blue', icon='info-sign')
        ).add_to(m)

    # Plot the last point with a special marker (e.g., red)
    last_point = trajectory_df.iloc[-1]
    folium.Marker(
        location=[last_point['Latitude'], last_point['Longitude']],
        popup=f"End: {last_point['Timestamp']}",
        icon=folium.Icon(color='red', icon='stop')
    ).add_to(m)

    return m

def get_geospatial_graph(current_location, next_location, padding=0.01):
    # Unpack the latitude and longitude from each location
    lat1, lon1 = current_location
    lat2, lon2 = next_location
    
    # Calculate the min and max latitudes and longitudes to form the bounding box
    min_lat = min(lat1, lat2) - padding
    max_lat = max(lat1, lat2) + padding
    min_lon = min(lon1, lon2) - padding
    max_lon = max(lon1, lon2) + padding
    
    # Get the street network within the bounding box
    G = ox.graph_from_bbox(max_lat, min_lat, max_lon, min_lon, network_type='drive')

    return G

def find_nearest_graph_node(graph, point):
    # Find the nearest network node to the given coordinates
    nearest_node = ox.distance.nearest_nodes(graph, point[0], point[1])

    return nearest_node
    
def convert_node_to_coords(graph, node):
    node_data = graph.nodes[node]
    location_latitude, location_longitude = node_data['y'], node_data['x']

    return (location_latitude, location_longitude)

def find_best_path(graph, node1, node2):
        best_path = nx.shortest_path(graph, source=node1, target=node2)

        return best_path

############################################################################################################
