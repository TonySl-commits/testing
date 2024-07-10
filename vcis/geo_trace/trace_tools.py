from tracemalloc import start
import pandas as pd
import numpy as np
import osmnx as ox
import networkx as nx
import folium
from folium.plugins import AntPath
from shapely.geometry import Point, LineString
from shapely.ops import nearest_points
from IPython.display import display
from datetime import datetime, timedelta
import numpy as np
import geopy.distance

from vcis.utils.properties import CDR_Properties

class TraceTools:

    def __init__(self):
        """Initialize class."""
        self.properties = CDR_Properties()

    def get_geospatial_graph(self, point1, point2, padding = 0.02, network_type='drive', simplify=False):
        # Unpack the latitude and longitude from each location
        lat1, lon1 = point1
        lat2, lon2 = point2
        
        # Calculate the min and max latitudes and longitudes to form the bounding box
        min_lat = min(lat1, lat2) - padding
        max_lat = max(lat1, lat2) + padding
        min_lon = min(lon1, lon2) - padding
        max_lon = max(lon1, lon2) + padding
        
        # Get the street network within the bounding box
        G = ox.graph_from_bbox(max_lat, min_lat, max_lon, min_lon, network_type=network_type, simplify=simplify)

        return G
    
    def get_geo_graph(self, data, padding = 0.02, network_type='drive', simplify=False):
        # Calculate the min and max latitudes and longitudes to form the bounding box
        min_lat = data['Latitude'].min() - padding
        max_lat = data['Latitude'].max() + padding
        min_lon = data['Longitude'].min() - padding
        max_lon = data['Longitude'].max() + padding

        # Get the street network within the bounding box
        G = ox.graph_from_bbox(max_lat, min_lat, max_lon, min_lon, network_type=network_type, simplify=simplify)

        return G

    def find_nearest_graph_node(self, graph, point):
        # Find the nearest network node to the given coordinates
        nearest_node = ox.distance.nearest_nodes(graph, point[1], point[0])

        return nearest_node
    
    def find_nearest_graph_edge(self, graph, point):
        # Find the nearest edge to the specified point
        nearest_edge = ox.distance.nearest_edges(G, X=point[1], Y=point[0])
        
        return nearest_edge

    def find_best_path(self, graph, node1, node2):
        best_path = nx.shortest_path(graph, source=node1, target=node2)

        return best_path
    
    def constrained_shortest_path(graph, start_node, end_node, through_edge):
        u, v = through_edge[0], through_edge[1]

        # Shortest path from start_node to u (one end of the specified edge)
        path_to_u = nx.shortest_path(graph, source=start_node, target=u, weight='length')

        # Shortest path from v (other end of the specified edge) to end_node
        path_from_v = nx.shortest_path(graph, source=v, target=end_node, weight='length')

        # Combine the paths, ensuring not to repeat the node v
        path = path_to_u + path_from_v

        return path

    def get_path_coordinates(self, G, path):
        # Use list comprehension to get coordinates of each node in the path
        coordinates = [(G.nodes[node]['y'], G.nodes[node]['x']) for node in path]
        return coordinates

    def check_edge_existence(self, graph, node1, node2):
        return graph.has_edge(node1, node2)

    def get_geo_trace_path_nodes(self, df):

        # get the min and max lat from the dataframe
        print("started G graph")
        graph = self.get_geo_graph(df)
        print("ended G graph")

        all_paths = []
        all_path_coords = []
        results_df = pd.DataFrame()

        # Process in chunks of 'interval'
        for start_idx in range(0, len(df)-1):
            end_idx = start_idx + 1
            start_point = (df.iloc[start_idx]['Latitude'], df.iloc[start_idx]['Longitude'])
            end_point = (df.iloc[end_idx]['Latitude'], df.iloc[end_idx]['Longitude'])
            
            # Find the nearest nodes to the start and end points
            start_node = self.find_nearest_graph_node(graph, start_point)
            end_node = self.find_nearest_graph_node(graph, end_point)
            
            # Find the shortest path between these nodes
            path = self.find_best_path(graph, start_node, end_node)
            
            # Get the data from the best path
            street_names, street_lengths = self.get_data_from_best_path(graph, path)
            
            # Convert the nodes in the path back to coordinates
            path_coords = self.get_path_coordinates(graph, path)
            
            # Generate dummy timestamps starting from the current time
            timestamps = [datetime.now() + timedelta(seconds=i*20) for i in range(len(path_coords))]
            
            # Create a DataFrame with the path coordinates and timestamps
            df_segment = pd.DataFrame(path_coords, columns=['Latitude', 'Longitude'])
            df_segment['Timestamp'] = timestamps
            df_segment['Street_Name'] = street_names
            df_segment['Street_Length'] = street_lengths
            df_segment['Cumulative_Length'] = df_segment['Street_Length'].cumsum()

            # Append the path and DataFrame
            all_paths.append(path)
            all_path_coords.extend(path_coords)
            results_df = pd.concat([results_df, df_segment])
            
        return results_df, all_path_coords, graph
    
    def get_geo_trace_path_edges(self, df):
        
        # get the min and max lat from the dataframe
        graph = self.get_geo_graph(df)
        
        all_paths = []
        all_path_coords = []
        results_df = pd.DataFrame()

        for start_idx in range(0, len(df) - 1):
            end_idx = start_idx + 1
            start_point = (df.iloc[start_idx]['Latitude'], df.iloc[start_idx]['Longitude'])
            end_point = (df.iloc[end_idx]['Latitude'], df.iloc[end_idx]['Longitude'])
            
            # Find the nearest edges to the start and end points
            edge = ox.distance.nearest_edges(graph, [start_point[1], end_point[1]], [start_point[0], end_point[0]])
            
            # Use one of the nodes from each edge to find the path
            print(edge)
            start_node = edge[0][0]  # Choosing the target node of the start edge
            end_node = edge[0][1]      # Choosing the target node of the end edge
            
            path = self.find_best_path(graph, start_node, end_node)
            
            street_names, street_lengths = self.get_data_from_best_path(graph, path)
            
            path_coords = self.get_path_coordinates(graph, path)
            
            timestamps = [datetime.now() + timedelta(seconds=i*20) for i in range(len(path_coords))]
            
            df_segment = pd.DataFrame(path_coords, columns=['Latitude', 'Longitude'])
            df_segment['Timestamp'] = timestamps
            df_segment['Street_Name'] = street_names
            df_segment['Street_Length'] = street_lengths
            df_segment['Cumulative_Length'] = df_segment['Street_Length'].cumsum()

            all_paths.append(path)
            all_path_coords.extend(path_coords)
            results_df = pd.concat([results_df, df_segment])

        return results_df, all_path_coords, graph


    def draw_map(self, data, coords, number:int = 1):
        # Create a Folium map centered around the first point in your DataFrame
        m = folium.Map(location=[data['Latitude'].iloc[0], data['Longitude'].iloc[0]], zoom_start=15)

        folium.PolyLine(coords).add_to(m)
        
        m.save(f"C:/Users/mpedro/Desktop/CDR_Trace/data/maps/test_maps/test{number}.html")
        
    def haversine_distance(self, lat1, lon1, lat2, lon2):

        lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1

        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        r = 6371000  # Radius of Earth in meters

        return c * r
    
    def get_data_from_best_path(self, G, best_path):

        best_path = list(zip(best_path[:-1], best_path[1:]))
        street_names = []
        street_length = []


        # Iterate over pairs of nodes in the path to access the edges that connect them
        for (u, v) in best_path:
            if G.has_edge(u, v):
                edge_data = G.get_edge_data(u, v, 0)

                # Check if the edge has a street name
                if 'name' in edge_data.keys() :
                    street_names.append(edge_data['name'])
                else:
                    street_names.append(None)

                if 'length' in edge_data.keys() :
                    street_length.append(edge_data['length'])
                else:
                    street_length.append(0)
            else:
                street_names.append(None)
                street_length.append(0)
        
        # Append None for the last coordinate which does not have an outgoing edge
        street_names.append(None)
        street_length.append(0)   

        return street_names, street_length
        
    def plot_geo_data(self, data, map):
        # Iterate over the rows and plot the real geospatial data on the map
        for index, row in data.iterrows():
            folium.Marker([row['Latitude'], row['Longitude']], popup=f"Time: {row['Timestamp']}").add_to(map)
            
        return map
    
    def plot_geo_trace(self, data, info):
        m = folium.Map(location=[data['Latitude'].iloc[0], data['Longitude'].iloc[0]], zoom_start=15)

        # Plot start marker
        folium.Marker([data['Latitude'].iloc[0], data['Longitude'].iloc[0]], 
                    icon=folium.Icon(color='green'), 
                    popup=f"Start Point: ({data['Latitude'].iloc[0]}, {data['Longitude'].iloc[0]})").add_to(m)
        
        # Plot end marker
        folium.Marker([data['Latitude'].iloc[-1], data['Longitude'].iloc[-1]], 
            icon=folium.Icon(color='red'), 
            popup=f"End Point: ({data['Latitude'].iloc[-1]}, {data['Longitude'].iloc[-1]})<br>").add_to(m)
        
        # Plot real geospatial data
        for index, row in data.iterrows():
            folium.CircleMarker(
                location=[row['Latitude'], row['Longitude']],
                radius= 15 if row["Cluster"]!= -1 else 10,
                popup=f"Latitude: {row['Latitude']}<br>"
                    f"Longitude: {row['Longitude']}<br>",
                    # f"Start Time: {row['Start_Time']}<br>"
                    # f"End Time: {row['End_Time']}<br>",
                color='red' if row["Cluster"]!= -1 else "green",
                fill=True,
            ).add_to(m)
        
        # folium.Marker([data['Latitude'].iloc[-1], data['Longitude'].iloc[-1]], 
        #             icon=folium.Icon(color='red'), 
        #             popup=f"End Point: ({data['Latitude'].iloc[-1]}, {data['Longitude'].iloc[-1]})<br>Distance Traveled: {data['Cumulative_Length'].iloc[-1]/1000:.2f} km").add_to(m)


        # # Add Road PolyLine
        # if coords is not None:
        #     folium.PolyLine(coords, color='darkblue').add_to(m)
            
        # # Plot real geospatial data
        # m = self.plot_geo_data(data, m)

        # # Place a marker at the first occurrence of each street name and then every 500 meters
        # for street, group in data.groupby('Street_Name'):
        #     first_row = group.iloc[0]
        #     folium.Marker(
        #         [first_row['Latitude'], first_row['Longitude']],
        #         icon=folium.Icon(icon='info-sign', color='blue'),
        #         popup=f"Street: {street}<br>Distance: {first_row['Cumulative_Length']/1000:.2f} km"
        #     ).add_to(m)

        #     cumulative_length_at_first_marker = first_row['Cumulative_Length']
        #     for _, row in group.iterrows():
        #         distance_since_last_marker = row['Cumulative_Length'] - cumulative_length_at_first_marker
        #         if distance_since_last_marker >= distance_threshold:
        #             folium.Marker(
        #                 [row['Latitude'], row['Longitude']],
        #                 icon=folium.Icon(icon='info-sign', color='blue'),
        #                 popup=f"Street: {street}<br>Distance: {row['Cumulative_Length']/1000:.2f} km"
        #             ).add_to(m)
        #             cumulative_length_at_first_marker = row['Cumulative_Length']

        m.save(f"C:/Users/mpedro/Desktop/CDR_Trace/data/maps/test_points/Showing_Points_test_{info}.html")
        
        return m           
    def validate_trace_results(self, trace_result, info):

        # Create folium map
        m = folium.Map(location=[trace_result['Latitude'].iloc[0], trace_result['Longitude'].iloc[0]], zoom_start=15)

        # Find the first valid location for the start marker
        if not trace_result['Latitude'].isna().all() and not trace_result['Longitude'].isna().all():
            first_valid = trace_result.dropna(subset=['Latitude', 'Longitude']).iloc[0]
            folium.Marker(
                [first_valid['Latitude'], first_valid['Longitude']],
                icon=folium.Icon(color='green'),
                popup=f"Start Point: ({first_valid['Latitude']}, {first_valid['Longitude']})"
            ).add_to(m)

        # Find the last valid location for the end marker
        if not trace_result['Latitude'].isna().all() and not trace_result['Longitude'].isna().all():
            last_valid = trace_result.dropna(subset=['Latitude', 'Longitude']).iloc[-1]
            folium.Marker(
                [last_valid['Latitude'], last_valid['Longitude']],
                icon=folium.Icon(color='red'),
                popup=f"End Point: ({last_valid['Latitude']}, {last_valid['Longitude']})"
            ).add_to(m)
        
        # Check if 'Gap' column exists in DataFrame
        has_gap_column = 'Gap' in trace_result.columns

        # Iterate over the rows of the trace_result
        for i, row in trace_result.iterrows():
            # Add real geospatial data, if available
            if row[['Latitude', 'Longitude']].notna().all():
                folium.CircleMarker(
                    location=[row['Latitude'], row['Longitude']],
                    radius= 10 if row['NodeType'] != "Stay Point" else 15,
                    popup=f"Latitude: {row['Latitude']}<br>"
                        f"Longitude: {row['Longitude']}<br>"
                        f"Count: {i}",
                    color='green' if row['NodeType'] != "Stay Point" else 'red',
                    fill=True,
                ).add_to(m)
                
            # Add traced geospatial data
            folium.CircleMarker(
                location=[row['NodeLatitude'], row['NodeLongitude']],
                radius= 5, 
                popup=f"Latitude: {row['NodeLatitude']}<br>"
                    f"Longitude: {row['NodeLongitude']}<br>"
                    f"Count: {i}",
                color='red',
                fill=True,
            ).add_to(m)

            if has_gap_column:
                if row['Gap'] == 1:
                    folium.Marker([row['NodeLatitude'], row['NodeLongitude']], 
                                  popup=f"Here was a Gap in time.<br>"
                                        f"Count: {i}",
                                  icon=folium.Icon(color='orange', icon='info-sign')
                    ).add_to(m) 
                elif row['Gap'] == 2:
                    folium.Marker([row['NodeLatitude'], row['NodeLongitude']], 
                                  popup=f"Here was a Gap in time.<br>"
                                        f"Count: {i}",
                                  icon=folium.Icon(color='pink', icon='info-sign')
                    ).add_to(m)
        
        # Plot the traced route using AntPath
        route_coords = list(zip(trace_result['NodeLatitude'], trace_result['NodeLongitude']))

        AntPath(
            locations=route_coords,
            color='darkblue',  # Set the path color to dark blue
            delay=1000,  # Set the animation delay (in milliseconds)
            dash_array=[10, 20],  # Adjust the dash pattern (optional)
            pulseColor = "#DDDDDD",
            opacity = 0.8
        ).add_to(m)

        m.save(f"C:/Users/mpedro/Desktop/CDR_Trace/data/maps/test_node_code/full_antpath_{info}.html")  
    
    def display_next_location_trajectory_antpath(self, data, route_coords, info):

        try:
            m = folium.Map(location=[data['Latitude'].iloc[0], data['Longitude'].iloc[0]], zoom_start=15)

            # Add start and end markers
            folium.Marker([data['Latitude'].iloc[0], data['Longitude'].iloc[0]], 
                        icon=folium.Icon(color='green'), 
                        popup=f"Start Point: ({data['Latitude'].iloc[0]}, {data['Longitude'].iloc[0]})"
                        ).add_to(m)
            
            folium.Marker([data['Latitude'].iloc[-1], data['Longitude'].iloc[-1]], 
                        icon=folium.Icon(color='red'), 
                        popup=f"End Point: ({data['Latitude'].iloc[-1]}, {data['Longitude'].iloc[-1]})<br>"
                        ).add_to(m)
            
            for i, row in data.iterrows():
                folium.CircleMarker(
                    location=[row['Latitude'], row['Longitude']],
                    radius= 15 if row['Cluster'] != -1 else 5,
                    popup=f"Latitude: {row['Latitude']}<br>"
                        f"Longitude: {row['Longitude']}<br>"
                        f"Count: {i}",
                    color='red' if row['Cluster'] != -1 else 'green',
                    fill=True,
                ).add_to(m)
            
            # Use AntPath to plot the route
            AntPath(
                locations=route_coords,
                color='darkblue',  # Set the path color to dark blue
                delay=1000,  # Set the animation delay (in milliseconds)
                dash_array=[10, 20],  # Adjust the dash pattern (optional)
                pulseColor = "#DDDDDD",
                opacity = 0.8
            ).add_to(m)

            m.save(f"C:/Users/mpedro/Desktop/CDR_Trace/data/maps/test_node_code/full_antpath-edges-{info}.html")
        
        except Exception as e:
            print(e)
            return "No route found"


    def display_next_location_trajectory_antpath_test(self, data):

            m = folium.Map(location=[data['Latitude'].iloc[0], data['Longitude'].iloc[0]], zoom_start=15)

            # Add start and end markers
            folium.Marker([data['Latitude'].iloc[0], data['Longitude'].iloc[0]], 
                        icon=folium.Icon(color='green'), 
                        popup=f"Start Point: ({data['Latitude'].iloc[0]}, {data['Longitude'].iloc[0]})"
                        ).add_to(m)
            
            folium.Marker([data['Latitude'].iloc[-1], data['Longitude'].iloc[-1]], 
                        icon=folium.Icon(color='red'), 
                        popup=f"End Point: ({data['Latitude'].iloc[-1]}, {data['Longitude'].iloc[-1]})<br>"
                        ).add_to(m)
            
            for i, row in data.iterrows():
                folium.CircleMarker(
                    location=[row['Latitude'], row['Longitude']],
                    radius= 15 if row['Cluster'] != -1 else 5,
                    popup=f"Latitude: {row['Latitude']}<br>"
                        f"Longitude: {row['Longitude']}<br>"
                        f"Count: {i}",
                    color='red' if row['Cluster'] != -1 else 'blue',
                    fill=True,
                ).add_to(m)

            m.save(f"C:/Users/mpedro/Desktop/CDR_Trace/data/maps/test_maps/full_antpath-zaherrrrrr_reduced.html")

    def display_next_location_trajectory_antpath_splitted(self, data, route_coords, info):
        m = folium.Map(location=[data['Latitude'].iloc[0], data['Longitude'].iloc[0]], zoom_start=15)

        # Add start and end markers
        folium.Marker([data['Latitude'].iloc[0], data['Longitude'].iloc[0]], 
                    icon=folium.Icon(color='green'), 
                    popup=f"Start Point: ({data['Latitude'].iloc[0]}, {data['Longitude'].iloc[0]})"
                    ).add_to(m)

        folium.Marker([data['Latitude'].iloc[-1], data['Longitude'].iloc[-1]], 
                    icon=folium.Icon(color='red'), 
                    popup=f"End Point: ({data['Latitude'].iloc[-1]}, {data['Longitude'].iloc[-1]})"
                    ).add_to(m)

        # Plot each segment between consecutive points using AntPath
        for i in range(len(route_coords) - 1):
            segment = [route_coords[i], route_coords[i+1]]
            AntPath(
                locations=segment,
                color='darkblue',  # Set the path color
                delay=1000,  # Animation delay
                dash_array=[10, 20],  # Dash pattern
                pulseColor="#DDDDDD",
                opacity=0.8
            ).add_to(m)

            # Optional: Add markers or popups for each step
            folium.Marker(route_coords[i], icon=folium.Icon(color='blue', icon='info-sign'),
                        popup=f"Step {i} from ({route_coords[i][0]}, {route_coords[i][1]})"
                                f" to ({route_coords[i+1][0]}, {route_coords[i+1][1]})").add_to(m)

        m.save(f"C:/Users/mpedro/Desktop/CDR_Trace/data/maps/test_maps/full_antpath-{info}.html")