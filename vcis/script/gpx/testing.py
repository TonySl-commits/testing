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

utils = CDR_Utils()
properties = CDR_Properties()
gpx_monitor_preprocess = GPXPreprocessMonitor()
correlation_functions = CDRCorrelationFunctions()

# Specify Start and End date for Geospatial data
start_date = "2023-12-19"
end_date = "2023-12-22"
geo_id = '8e09f8c4-7158-4e15-accf-14df14740190'

# Convert them to milliseconds
start_date = correlation_functions.str_date_to_millisecond(start_date)
end_date = correlation_functions.str_date_to_millisecond(end_date)

# Get Geospatial data from Cassandra
df_geo = utils.get_geo_device_history(device_id = geo_id , start_date= start_date , end_date= end_date , server="10.1.10.66")

intervals = gpx_monitor_preprocess.find_intervals(df_geo, 5)

print(intervals[0]["usage_timeframe"])