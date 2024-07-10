
import gpxpy
import folium
import pandas as pd
import datetime
import time
import os
import uuid

from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.gpx.gpx_preproccess_monitor import GPXPreprocessMonitor

utils = CDR_Utils()
properties = CDR_Properties()
gpx_monitor_preprocess = GPXPreprocessMonitor(cassandra_write=True)

random_device_id = str(uuid.uuid4())

first_digit = '8'
last_digit = '90'
# server = '10.10.10.101'
offset_hours = 2
server = '10.1.10.66'

# device_id = first_digit + random_device_id[1:-2] + last_digit
device_id = "8354490f-a78c-4807-9cf4-fc54d46a3190"
print(device_id)

# Process all GPX files in the folder

df_gpx  = gpx_monitor_preprocess.read_gpx_folder()


df_gpx = gpx_monitor_preprocess.process_gpx_folder(df_gpx,device_id,offset_hours=offset_hours)

df_gpx.to_csv(properties.passed_filepath_gpx_csv + "gpx.csv", index=False)

# map.save( properties.passed_filepath_gpx_map + f"map_{device_id}.html")

gpx_monitor_preprocess.write_into_cassandra(df_gpx,cassandra_host = server)

print("Done")

