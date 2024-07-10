import pandas as pd

import warnings
warnings.filterwarnings('ignore')

from IPython.display import display

import datetime
from cdr_trace.geo_trace.geo_trace import GeoTrace
from cdr_trace.geo_trace.trace_tools import TraceTools
from cdr_trace.geo_trace.geo_edge_trace.geo_node_list import GeoNodeList

print('INFO:  Geo Trace Engine Process Started !!!')

trace_tools = TraceTools()
geo_node_list = GeoNodeList()

data_directory = "/u01/jupyter-scripts/Chris.y/CDR_trace/Trace/data/dataframes/"

# Trace the device path
data = pd.read_csv(data_directory + "test_df/geo_node_list_trace_data.csv")
data.drop(columns=['AdjustedLongitude', 'AdjustedLatitude', 'PreviousNode', 'NewNode', 'NewNodeLatitude', 'NewNodeLongitude', 'NodeLatitude', 'NodeLongitude'], inplace=True)
        
print('INFO:  Initializing Graph !!!')

graph = trace_tools.get_geo_graph(data)
geo_node_list.set_graph(graph)

print('INFO:  Started Creating Geo Node List !!!')

geo_node_list.create_node_list(data)

# geo_node_list.validate_path()

trace_result = geo_node_list.get_trace_result()
trace_result = geo_node_list.format_trace_result(trace_result)

# trace_result = pd.read_csv(data_directory + "test_df/geo_node_list_trace_result.csv")

map_output_directory = "/u01/jupyter-scripts/Chris.y/CDR_trace/Trace/data/maps/test_maps/"
trace_tools.validate_trace_results(trace_result, map_output_directory, "geo_node_list_trace_result.html")

# trace_result = geo_node_list.find_street_name(trace_result)
# print(trace_result)

# # Save trace result

# trace_result.to_csv(data_directory + "test_df/geo_node_list_trace_result.csv", index=False)
# trace_result.drop(columns=['StreetLength'], inplace=True)

# trace_result.to_json(data_directory + "new_trace_old_format.json", orient='records', force_ascii=False)

print('INFO:  Geo Trace Engine Process Successfully Completed!!!')


##########################################################################################################