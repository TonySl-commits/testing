import json
import pandas as pd

import warnings
warnings.filterwarnings('ignore')

from IPython.display import display

import datetime
from cdr_trace.geo_trace.geo_trace import GeoTrace
from cdr_trace.geo_trace.trace_tools import TraceTools

print('INFO:       Geo Trace Engine Process Started !!!')

geo_tracer = GeoTrace()
trace_tools = TraceTools()

# data_directory = "C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/data_simplified/"
data_directory = "C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/"

df_history_marcelinho = pd.read_csv(data_directory + "device_history_marcelinho.csv")
df_history_pedro = pd.read_csv(data_directory + "device_history_pedro.csv")
df_history_nathalie = pd.read_csv(data_directory + "device_history_nathalie.csv")
df_history_zaher = pd.read_csv(data_directory + "device_history_zaher.csv")
geo_id_from_fadi_device1 = pd.read_csv(data_directory + "device_history_from_fadi_device1.csv")
geo_id_from_fadi_device2 = pd.read_csv(data_directory + "device_history_from_fadi_device2.csv")

# df_history_nathalie['service_provider_id'] = 'batenjen'
# df_history_nathalie['location_name'] = 'kebbe'

# print(df_history_nathalie2.shape)

# trace_tools.display_next_location_trajectory_antpath_test_00(df_history_nathalie2)

# Filter date on 2024-01-13 only (from Timestamp) 2024-01-13 23:22:07 - 2024-01-14 00:38:11
# df_history_nathalie['Date'] = pd.to_datetime(df_history_nathalie['Timestamp'], unit='ms').dt.date
# filter_date = datetime.date(2024, 1, 13)
# df_history_nathalie = df_history_nathalie[df_history_nathalie['Date'] == filter_date]

# Filter device
# df_history_nathalie = df_history_nathalie[df_history_nathalie['device_id'] == '4e79560f-e59a-4d7b-8b91-6dddbd571c57']

# Filter time
# df_history_nathalie['Timestamp'] = pd.to_datetime(df_history_nathalie['Timestamp'], unit='ms')
# start_time = datetime.datetime(2024, 1, 14, 10, 12, 48)
# end_time = datetime.datetime(2024, 1, 14, 12, 28, 15)
# df_history_nathalie = df_history_nathalie[(df_history_nathalie['Timestamp'] >= start_time) & (df_history_nathalie['Timestamp'] <= end_time)]

# df_history_nathalie = df_history_nathalie.head(50)

# # Trace the device path
# data = geo_tracer.trace_device_path(df_history_nathalie)

# all_paths = geo_tracer.get_all_paths_from_dictionaries(data)

# all_paths.to_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/all_paths_zaher.csv", index=False)

# all_paths.to_json("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/all_paths_zaher.json", orient='records', force_ascii=False)

nathalie_paths = pd.read_csv(data_directory + "all_paths_nathalie.csv")
nathalie_paths.drop(columns=['Cumulative_Length'], inplace=True)

# graph = trace_tools.get_geo_graph(nathalie_paths)
# lat_col_index = nathalie_paths.columns.get_loc('Latitude')
# lon_col_index = nathalie_paths.columns.get_loc('Longitude')
# nathalie_paths['Node'] = nathalie_paths.apply(lambda row: trace_tools.find_nearest_graph_node(graph, (row.iloc[lat_col_index], row.iloc[lon_col_index])), axis=1)
# print(nathalie_paths)

# Group by Latitude and Longitude
# Correct approach with groupby and aggregate
result_df = nathalie_paths.groupby(['Latitude', 'Longitude']).agg({
    'Timestamp': lambda x: list(x),
    'Street_Name': lambda x: next((item for item in x if item is not None), ''),
    'Street_Length': lambda x: next((item for item in x if item > 0), 0)
}).reset_index()

print(result_df)

result_df.to_json("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/all_paths_nathalie2.json", orient='records', force_ascii=False)

print('INFO:       Geo Trace Engine Process Successfully Completed!!!')


####################################################
# ######################################################

'''
2024-01-13 18:10:27 - 2024-01-13 20:30:08

2024-01-14 10:12:48 - 2024-01-14 12:28:15
'''