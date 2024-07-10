import pandas as pd
import time
import folium

from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.nodes.nodes_functions import Nodes
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.plots.plots_tools import PlotsTools
import plotly.graph_objects as go

utils = CDR_Utils()
properties = CDR_Properties()
cassandra_tools = CassandraTools()
cassandra_spark_tools = CassandraSparkTools()
nodes_functions = Nodes(verbose=True)
plots= PlotsTools()
time1 = time.time()

# # Specify Start and End date for Geospatial data
start_date = "2023-03-01"
end_date = "2024-01-30"
geo_id = "8e09f8c4-7158-4e15-accf-14df14740190"
imsi_id = "121415223435890"
server = "10.1.10.66"
distance=30
local = None


# df_imsi = cassandra_spark_tools.get_device_history_imsi_spark(imsi_id= imsi_id , start_date= start_date , end_date= end_date,local =local)
# df_imsi = utils.convert_ms_to_datetime(df_imsi)

start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(start_date)

# # Get Geospatial and CDR data from Cassandra
df_geo = cassandra_tools.get_device_history_geo(device_id = geo_id , start_date= start_date , end_date= end_date , server="10.1.10.66")
print(df_geo.head())

df_near_nodes = nodes_functions.get_close_nodes(data=df_geo,distance= distance,passed_server=server)
    
print(df_near_nodes.head())


df_gps_nodes = nodes_functions.get_gps_nodes(df_near_nodes)

df_gps_nodes = nodes_functions.get_possible_cgi_id(df_gps_nodes)

df_gps_nodes = nodes_functions.get_bts_location(df_gps_nodes)
df_gps_nodes.drop('distance',axis=1,inplace=True)

df_gps_nodes.to_csv(properties.passed_filepath_excel + 'df_gps_nodes.csv',index=False)
# df_gps_nodes = pd.read_csv(properties.passed_filepath_excel + 'df_gps_nodes.csv')
# plots.bts_nodes_cgi_plot(df_gps_nodes)

df_connected_devices = nodes_functions.get_connected_devices(df_gps_nodes)

print(df_connected_devices)

