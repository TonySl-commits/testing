import gpxpy
import folium
import pandas as pd
import datetime
from datetime import datetime
import time
import os
import ast

from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.script.nodes.explode_function import Explode
from vcis.databases.cassandra.cassandra_tools import CassandraTools


utils = CDR_Utils()
properties = CDR_Properties()
explode_functions = Explode()
cassandra_tools = CassandraTools()

time1 = time.time()

# Specify Start and End date for Geospatial data
start_date = "2023-01-01"
end_date = "2025-01-01"
geo_id = "22e61cc2-fc65-492f-b822-a708d15d1b80"
imsi_id = "302147251006280"

# Convert them to milliseconds
start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(start_date)

# Get Geospatial and CDR data from Cassandra
df_geo = cassandra_tools.get_device_history_geo(device_id = geo_id , start_date= start_date , end_date= end_date , server="10.1.10.66")
# df_imsi = utils.get_cdr_imsi_history(imsi_id=imsi_id, start_date=start_date, end_date=end_date)


print("Started exploding")
# Get exploded nodes dataset
df_nodes = explode_functions.explode(properties.bts_table_name_with_nodes)
print("Finished exploding")


step = utils.get_step_size(20)
print(f"Step size is {step}")

df_geo = utils.binning(data = df_geo, step = step)
df_nodes = utils.binning(data = df_nodes, step = step)

df_geo = utils.combine_coordinates(data = df_geo)
df_nodes = utils.combine_coordinates(data = df_nodes)

# Merging geo data and cdr data on the 
merged_df = pd.merge(df_geo,df_nodes,on='grid')

print(merged_df)
print(f"These are the Merged df columns: {merged_df.columns}")

time2 = time.time()
End_time = time2 - time1
print(f"Function completed in {round(End_time, 3)} seconds")

# merged_df.to_excel('merged.xlsx')
