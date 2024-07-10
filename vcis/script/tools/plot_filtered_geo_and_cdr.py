
import gpxpy
import folium
import pandas as pd
import datetime
from datetime import datetime
import time
import os

from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.gpx.gpx_preproccess_monitor import GPXPreprocessMonitor
from vcis.correlation.cdr_correlation_functions import CDRCorrelationFunctions
from vcis.plots.plots_tools import  PlotsTools
from vcis.databases.cassandra.cassandra_tools import CassandraTools

utils = CDR_Utils()
properties = CDR_Properties()
gpx_monitor_preprocess = GPXPreprocessMonitor()
correlation_functions = CDRCorrelationFunctions()
plotstools = PlotsTools()
cassandra_tools = CassandraTools()

# Specify Start and End date for Geospatial data
start_date = "2023-12-19"
end_date = "2023-12-22"
geo_id = '8e09f8c4-7158-4e15-accf-14df14740190'
imsi_id = "151475729807890"
# difference_hours = 2

# Convert them to milliseconds
start_date = correlation_functions.str_date_to_millisecond(start_date)
end_date = correlation_functions.str_date_to_millisecond(end_date)

# Get Geospatial data from Cassandra
df_geo = cassandra_tools.get_device_history_geo(device_id = geo_id , start_date= start_date , end_date= end_date , server="10.1.10.66")

# Get map where geo data are plotted
map = gpx_monitor_preprocess.plot_gpx_folium(df_geo)

# Function to Find Intervals with Specified Time Differences (in hours)
intervals = gpx_monitor_preprocess.find_intervals(df_geo, 2)

# Get final map where Geo and CDR  data are plotted
map = plotstools.get_correlation_plot_folium_filtered(start_date, end_date, intervals, map, imsi_id)

# df_geo.to_csv( properties.gpx_csv_folder_path + "gpx.csv", index=False)

# map.save( properties.gpx_map_folder_path + "map.html")

# gpx_monitor_preprocess.write_into_cassandra(df_gpx)

print("Done")


