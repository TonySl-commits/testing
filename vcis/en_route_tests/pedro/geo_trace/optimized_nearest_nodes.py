import osmnx as ox
import pandas as pd
import numpy as np
import math

import seaborn as sns
import matplotlib.pyplot as plt
from st_dbscan import ST_DBSCAN

from cdr_trace.geo_trace.trace_tools import TraceTools
# from cdr_trace.geo_trace.geo_edge_trace.geo_node_list import GeoNodeList
from cdr_trace.geo_trace.geo_node_list import GeoNodeList
from cdr_trace.geo_trace.geo_trace import GeoTrace
import time
import warnings
warnings.filterwarnings('ignore')

geo_trace = GeoTrace()
trace_tools = TraceTools()

data_name = "df_2024-04-17--15--47--08-2024-04-17--17--12--23"
data = pd.read_csv(f"C:/Users/yachristopher/Desktop/CDR_trace/data/device_history/{data_name}.csv")

# print(data)
# print(data.columns)

############################################################################################################

print("Started G graph creation")
time11 = time.time()

graph = trace_tools.get_geo_graph(data)

time12 = time.time()
print(f"Total time taken: {time12 - time11}")
print("Finished G graph creation")

############################################################################################################

# Calculate the time for the function to finish
time1 = time.time()
print("Started")

# Get the nearest node to each geospatial point from the graph
lat_col_index = data.columns.get_loc('Latitude')
lon_col_index = data.columns.get_loc('Longitude')
data['NewColumn'] = data.apply(lambda row: trace_tools.find_nearest_graph_node(graph, (row.iloc[lat_col_index], row.iloc[lon_col_index])), axis=1)
# data = trace_tools.find_nearest_graph_node(data, graph)

# Calculate end time
time2 = time.time()
print(f"Total time taken without apply: {time2 - time1}")

############################################################################################################
############################################################################################################
############################################################################################################
############################################################################################################

# Calculate the time for the function to finish
time3 = time.time()

# Get the list of points from the data
list_of_points = list(zip(data['Latitude'], data['Longitude']))

data["NewColumn_Apply"] = trace_tools.find_nearest_graph_node_list(graph, list_of_points)

# Calculate end time
time4 = time.time()
print(f"Total time taken with apply: {time4 - time3}")

############################################################################################################

print(data['NewColumn'])
print(data['NewColumn_Apply'])

print(data['NewColumn'] == data['NewColumn_Apply'])