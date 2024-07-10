import osmnx as ox
import pandas as pd
import numpy as np
import math
import networkx as nx

import seaborn as sns
import matplotlib.pyplot as plt
from st_dbscan import ST_DBSCAN

from cdr_trace.geo_trace.trace_tools import TraceTools
# from cdr_trace.geo_trace.geo_edge_trace.geo_node_list import GeoNodeList
from cdr_trace.geo_trace.geo_node_list import GeoNodeList
from cdr_trace.geo_trace.geo_trace import GeoTrace
from cdr_trace.geo_trace.geo_node import GeoNode
import time
import warnings
warnings.filterwarnings('ignore')

geo_trace = GeoTrace()
trace_tools = TraceTools()

data_name = "TraceResult_2024-04-17--15--47--08-2024-04-17--17--12--23"
data = pd.read_csv(f"C:/Users/yachristopher/Desktop/CDR_trace/data/device_history/{data_name}.csv")

data_dropped = data.drop(columns=['Unnamed: 0','NodeLatitude', 'NodeLongitude', 'NodeType', 'StreetName','StartTime','EndTime'])

print(data_dropped)
print(data_dropped.columns)

############################################################################################################

print("Started G graph creation")
time11 = time.time()

graph = trace_tools.get_geo_graph(data_dropped)

time12 = time.time()
print(f"Total time taken: {time12 - time11}")
print("Finished G graph creation")

############################################################################################################

# def find_nearest_node(graph, point, return_dist=True):
#     # Find the nearest network node to the given coordinates
#     nearest_node, distance = ox.distance.nearest_nodes(graph, point[1], point[0], return_dist=return_dist)

#     return nearest_node, distance

# def find_osmnx_node(graph, latitude, longitude):

#     node, distance = find_nearest_node(graph, (latitude, longitude))

#     # Get the node coordinates
#     node_latitude = graph.nodes[node]['y']
#     node_longitude = graph.nodes[node]['x']

#     return node_latitude,node_longitude

# # Calculate the time for the function to finish
# time1 = time.time()
# print("Started")

# for i, row in data.iterrows():
#     latitude = row["Latitude"]
#     longitude = row["Longitude"]

#     node_latitude, node_longitude = find_osmnx_node(graph, latitude, longitude)

#     # Store in separate columns
#     data.loc[i, "Nearest_Node_Latitude_before"] = node_latitude
#     data.loc[i, "Nearest_Node_Longitude_before"] = node_longitude

# # Calculate end time
# time2 = time.time()

# print(f"Total time taken without apply: {time2 - time1}")
# print(data[["Nearest_Node_Latitude_before", "Nearest_Node_Longitude_before"]])

# ############################################################################################################
# ############################################################################################################
# ############################################################################################################
# ############################################################################################################

# def find_nearest_node_list(graph, list_of_points):    
#     # Unpack the list of points into latitude and longitude lists
#     latitudes, longitudes = zip(*list_of_points)

#     # Use osmnx to find the nearest node for each point
#     nearest_nodes = ox.distance.nearest_nodes(graph, longitudes, latitudes)

#     return nearest_nodes

# def find_osmnx_node_list(graph, list_of_points):
#     node_ids = find_nearest_node_list(graph, list_of_points)

#     # Extract node attributes for all nodes at once
#     nodes_attributes = nx.get_node_attributes(graph, 'y')
#     node_latitudes = [nodes_attributes[node] for node in node_ids]
#     nodes_attributes = nx.get_node_attributes(graph, 'x')
#     node_longitudes = [nodes_attributes[node] for node in node_ids]

#     return node_latitudes, node_longitudes

# # Calculate the time for the function to finish
# time3 = time.time()

# # Get the list of points from the data
# list_of_points = list(zip(data['Latitude'], data['Longitude']))

# data["Nearest_Node_Latitude_before"], data["Nearest_Node_Longitude_before"] = find_osmnx_node_list(graph, list_of_points)

# # Calculate end time
# time4 = time.time()
# print(f"Total time taken with apply: {time4 - time3}")
# print(data[["Nearest_Node_Latitude_before", "Nearest_Node_Longitude_before"]])

############################################################################################################
############################################################################################################
############################################################################################################
############################################################################################################

start_time = time.time()

def create_node_register(graph):
    # Extracting nodes and their attributes from the graph
    node_data = graph.nodes(data=True)

    # Converting to a DataFrame
    node_data = pd.DataFrame(node_data)

    # Check if your DataFrame column names are set properly
    node_data.columns = ['Node_ID', 'Attributes']

    # Normalize the dictionary column into separate columns
    attributes_df = pd.json_normalize(node_data['Attributes'])

    # Concatenate the original Node_ID with the new attributes DataFrame
    node_register = pd.concat([node_data['Node_ID'], attributes_df], axis=1)

    # Renaming columns for clarity
    node_register.rename(columns={'y': 'NodeLatitude', 'x': 'NodeLongitude'}, inplace=True)

    # Set the index to Node_ID if it makes access easier
    node_register.set_index('Node_ID', inplace=True)

    # Filter columns
    node_register = node_register[['NodeLatitude', 'NodeLongitude']]

    return node_register

def filter_node_register(data):
    # Create a set of unique OSM_IDs from data
    osm_ids = set(data['OSM_ID'])

    # Filter node register based on unique IDs
    node_register = node_register[node_register.index.isin(osm_ids)].copy()

    return node_register


end_time = time.time()
print(f"Total time taken: {end_time - start_time}")

# # Resetting the index to turn the node IDs into a regular column
# # node_data.reset_index(inplace=True)
# # node_data.rename(columns={'index': 'node_ID'}, inplace=True)


print(filtered_df2)
print(filtered_df2.columns)
# print(node_data.shape)