from tracemalloc import start
import pandas as pd
import osmnx as ox
import networkx as nx
import folium
import ast
from IPython.display import display
from datetime import datetime, timedelta
from folium.plugins import TimestampedGeoJson, AntPath

import warnings
warnings.filterwarnings('ignore')

def get_geospatial_graph(current_location, next_location, padding=0.01, simplify=False):
    # Unpack the latitude and longitude from each location
    lat1, lon1 = current_location
    lat2, lon2 = next_location
    
    # Calculate the min and max latitudes and longitudes to form the bounding box
    min_lat = min(lat1, lat2) - padding
    max_lat = max(lat1, lat2) + padding
    min_lon = min(lon1, lon2) - padding
    max_lon = max(lon1, lon2) + padding
    
    # Get the street network within the bounding box
    G = ox.graph_from_bbox(max_lat, min_lat, max_lon, min_lon, network_type='drive', simplify=simplify)

    return G

def get_geo_graph(data, padding = 0.01, simplify=False):
        # Calculate the min and max latitudes and longitudes to form the bounding box
        min_lat = data['Latitude'].min() - padding
        max_lat = data['Latitude'].max() + padding
        min_lon = data['Longitude'].min() - padding
        max_lon = data['Longitude'].max() + padding

        # Get the street network within the bounding box
        G = ox.graph_from_bbox(max_lat, min_lat, max_lon, min_lon, network_type='drive', simplify=simplify)

        return G

def find_nearest_graph_node(graph, point):
    # Find the nearest network node to the given coordinates
    nearest_node = ox.distance.nearest_nodes(graph, point[1], point[0])

    return nearest_node

def find_best_path(graph, node1, node2):
    best_path = nx.shortest_path(graph, source=node1, target=node2)

    return best_path

def get_path_coordinates(G, path):
    # Use list comprehension to get coordinates of each node in the path
    coordinates = [(G.nodes[node]['y'], G.nodes[node]['x']) for node in path]
    return coordinates

def check_edge_existence(graph, node1, node2):
    return graph.has_edge(node1, node2)

def get_geo_trace_path(start_point, end_point, simplify=False):

    # Generate the geospatial graph for the area
    graph = get_geospatial_graph(start_point, end_point, simplify=simplify)
    
    # Find the nearest nodes to the start and end points
    start_node = find_nearest_graph_node(graph, start_point)
    end_node = find_nearest_graph_node(graph, end_point)
    
    # Find the shortest path between these nodes
    path = find_best_path(graph, start_node, end_node)

    # Get the data from the best path
    street_names, street_lengths = get_data_from_best_path(graph, path)
    
    # Convert the nodes in the path back to coordinates
    path_coords = get_path_coordinates(graph, path)
    
    # Generate dummy timestamps starting from the current time
    timestamps = [datetime.now() + timedelta(seconds=i*20) for i in range(len(path_coords))]
    
    # Create a DataFrame with the path coordinates and timestamps
    df = pd.DataFrame(path_coords, columns=['Latitude', 'Longitude'])
    
    df['Timestamp'] = timestamps
    df['Street_Name'] = street_names
    df['Street_Length'] = street_lengths
    df['Cumulative_Length'] = df['Street_Length'].cumsum()
    
    
    return df, path_coords, graph

def get_geo_trace_paths_zaher(df):

    # get the min and max lat from the dataframe
    print("started G graph")
    graph = get_geo_graph(df)
    print("ended G graph")

    all_paths = []
    all_path_coords = []
    results_df = pd.DataFrame()

    # Process in chunks of 'interval'
    for start_idx in range(0, len(df) - 1):
        end_idx = start_idx + 1
        start_point = (df.iloc[start_idx]['Latitude'], df.iloc[start_idx]['Longitude'])
        end_point = (df.iloc[end_idx]['Latitude'], df.iloc[end_idx]['Longitude'])
        
        # Find the nearest nodes to the start and end points
        start_node = find_nearest_graph_node(graph, start_point)
        end_node = find_nearest_graph_node(graph, end_point)
        
        # Find the shortest path between these nodes
        path = find_best_path(graph, start_node, end_node)
        
        # Get the data from the best path
        street_names, street_lengths = get_data_from_best_path(graph, path)
        
        # Convert the nodes in the path back to coordinates
        path_coords = get_path_coordinates(graph, path)
        
        # Extract real timestamps for start and end of the path
        start_timestamp = pd.to_datetime(df.iloc[start_idx]['Start_Time'])
        end_timestamp = pd.to_datetime(df.iloc[end_idx]['Start_Time'])
        
        # Create timestamp series
        timestamps = pd.date_range(start=start_timestamp, end=end_timestamp, periods=len(path_coords))
        
        # Create a DataFrame with the path coordinates and timestamps
        df_segment = pd.DataFrame(path_coords, columns=['Latitude', 'Longitude'])
        df_segment['Timestamp'] = timestamps
        df_segment['Street_Name'] = street_names
        df_segment['Street_Length'] = street_lengths
        df_segment['Cumulative_Length'] = df_segment['Street_Length'].cumsum()

        print("DF Segment:")
        print(df_segment)
        print(df_segment.columns)

        # Append the path and DataFrame
        all_paths.append(path)
        all_path_coords.extend(path_coords)
        results_df = pd.concat([results_df, df_segment])



    print("results_df:")
    print(results_df)
    print(results_df.columns)
    
    return results_df, all_path_coords, graph

def plot_folium_more(data, coords, distance_threshold, info):

    m = folium.Map(location=[data['Latitude'].iloc[0], data['Longitude'].iloc[0]], zoom_start=15)

    # Add start and end markers
    folium.Marker([data['Latitude'].iloc[0], data['Longitude'].iloc[0]], 
                  icon=folium.Icon(color='green'), 
                  popup=f"Start Point: ({data['Latitude'].iloc[0]}, {data['Longitude'].iloc[0]})").add_to(m)
    folium.Marker([data['Latitude'].iloc[-1], data['Longitude'].iloc[-1]], 
                  icon=folium.Icon(color='red'), 
                  popup=f"End Point: ({data['Latitude'].iloc[-1]}, {data['Longitude'].iloc[-1]})<br>"
                        # f"Distance Traveled: {data['Cumulative_Length'].iloc[-1]/1000:.2f} km"
                ).add_to(m)

    # Add PolyLines
    folium.PolyLine(coords, color='darkblue').add_to(m)

    # Place a marker at the first occurrence of each street name and then every 500 meters
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

    m.save(f"C:/Users/mpedro/Desktop/CDR_Trace/data/maps/test_maps/test{info}.html")
    
    return m

def plot_folium_single(data, info):

    # Create a Folium map centered around the first point in your DataFrame
    m = folium.Map(location=[data['Latitude'].iloc[0], data['Longitude'].iloc[0]], zoom_start=15)

    # Add circles for each point in the DataFrame
    for i, row in data.iterrows():
        folium.Marker(
            location=[row['Latitude'], row['Longitude']],
            # radius= 15 if row['Cluster'] != -1 else 5,
            popup=f"Latitude: {row['Latitude']}<br>"
                f"Longitude: {row['Longitude']}<br>"
                f"Count: {i}",
            # color='red' if row['Cluster'] != -1 else 'blue'
            # fill=True,
        ).add_to(m)

        # if row['Cluster'] != -1:
        #     folium.Marker(
        #         location=[row['Latitude'], row['Longitude']],
        #                 icon=folium.Icon(color='green'),
        #                 popup=(f"Start Time: {row['Start_Time']}<br>"
        #                         f"End Time: {row['End_Time']}<br>"
        #                         f"Duration: {row['Duration']}<br>"
        #                         f"Size: {row['Size']}")
        #     ).add_to(m)
    
    m.save(f"C:/Users/mpedro/Desktop/CDR_Trace/data/maps/test_maps/device_history{info}.html")

def display_next_location_trajectory_antpath(data, route_coords, info):

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
            color='red', # if row['Cluster'] != -1 else 'blue',
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

    m.save(f"C:/Users/mpedro/Desktop/CDR_Trace/data/maps/test_maps/full_antpath-{info}.html")

def get_data_from_best_path(G, best_path):

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
                


# Pedro Valoores
# df1, path_coords, G1 = get_geo_trace_path((33.965551, 35.627827), (33.890269, 35.558313))

# # Zaher df

output_directory = "C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/data_simplified/"

# df_zaher = pd.read_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/data_zaher_filtered.csv")
df_simplified = pd.read_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/data_simplified/simplified_data.csv")
# df_zaher = df_zaher.iloc[:1000]

# df_history = pd.read_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/device_history222/2023-04-18_f812f4e7-83fc-499b-9aab-04b3ac93307f.csv")

# # Rename columns
# df_history.rename(columns={
#         'location_latitude': 'Latitude', 
#         'location_longitude': 'Longitude', 
#         'usage_timeframe': 'Timestamp'
#     }, inplace=True)

# df_history = df_history[['device_id', 'Latitude' ,'Longitude', 'Timestamp']]

# df_zaher = df_zaher.sort_values(by='Timestamp').reset_index(drop=True)

# Convert 'Timestamp' from milliseconds to datetime
# df_zaher['Timestamp'] = pd.to_datetime(df_zaher['Timestamp'], unit='ms')

# plot_folium_single(df_zaher, "-device_history_map_nodes")

print("df_simplified")
print(df_simplified)
print(df_simplified.columns)

df1, path_coords, G1 = get_geo_trace_paths_zaher(df_simplified)
df1.reset_index(inplace=True, drop=True)

# df1.to_csv(output_directory + "data_zaher_geo.csv")

# df1["device_id"] = df_zaher["device_id"].values[0]
# df1.drop(columns=['Cumulative_Length'], inplace=True)

# df1 = df1[['device_id', 'Timestamp', 'Latitude', 'Longitude', 'Street_Name', 'Street_Length']]

# df1.to_csv(output_directory + "data_zaher_geo_complete.csv")


# display(df1)
# print(df1.columns)


# plot_folium_more(df1, path_coords, 500, "-device_history_map_nodes_path")
display_next_location_trajectory_antpath(df_simplified, path_coords, "-1-path")

# data = pd.read_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/data_zaher_geo_complete.csv")

# # print(data)

data.to_json(output_directory + "new_geo_trace.json", orient='values', force_ascii=False)


# display(df)




# df_zaher = pd.read_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/data_zaher_geo_simplified.csv")
# df_zaher = df_zaher.sort_values(by='Timestamp').reset_index(drop=True)

# plot_folium_single(df_zaher, "-1")


'''
Time interval: 2024-01-13 19:57:07 - 2024-01-13 21:29:38
Device: 436cab63-5002-475d-8d11-c321e5850659
'''






######################################################################################################################
######################################################################################################################
######################################################################################################################

# # device_history222 data instead of Zaher data
# df = pd.read_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/device_history_faraya.csv")

# # Convert 'usage_timeframe' to datetime
# df['usage_timeframe'] = pd.to_datetime(df['usage_timeframe'], unit='ms')

# # Sorting the DataFrame by 'device_id' and 'usage_timeframe'
# df = df.sort_values(by=['device_id', 'usage_timeframe'])

# # Group by 'device_id' and date from 'usage_timeframe'
# grouped = df.groupby(['device_id', df['usage_timeframe'].dt.date])

# for (device_id, date), group in grouped:
#     filename = f"C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/device_history222/{date}_{device_id}.csv"
#     group.to_csv(filename, index=False)


########

# import pandas as pd
# df_zaher = pd.read_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/data_zaher.csv")

# # Convert 'Timestamp' from milliseconds to datetime
# df_zaher['Timestamp'] = pd.to_datetime(df_zaher['Timestamp'], unit='ms')
# df_zaher = df_zaher.sort_values(by='Timestamp').reset_index(drop=True)

# # Define the time interval
# start_time = pd.to_datetime('2024-01-13 19:57:07')
# end_time = pd.to_datetime('2024-01-13 21:29:38')

# # Filter based on time interval and device_id
# filtered_df_zaher = df_zaher[
#     (df_zaher['Timestamp'] >= start_time) & 
#     (df_zaher['Timestamp'] <= end_time) & 
#     (df_zaher['device_id'] == '436cab63-5002-475d-8d11-c321e5850659')
# ]

# filtered_df_zaher.to_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/data_zaher_filtered.csv")



# My device id
'''
26e170d1-9bbc-4b1f-b3e9-4a7fd265a033
'''
