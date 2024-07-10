import numpy as np
import pandas as pd
import geopy.distance
import networkx as nx
import matplotlib.pyplot as plt
from shapely.geometry import Point, Polygon
import time
import warnings
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql import DataFrame as SparkDataFrame
from vcis.trajectory.trajectory_functions import Points_within_sector
from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties

utils = CDR_Utils()
properties = CDR_Properties()
pts_within_sec = Points_within_sector()

# Filter out FutureWarnings
warnings.simplefilter(action='ignore', category=FutureWarning)

start_time = time.time()

# data = utils.get_spark_data('loc_location_cdr_bts_new')
data = pd.read_excel(properties.works_on)
data = pts_within_sec.put_data_to_process(data)
data = data.head(10)

######### Method 1: Using Nodes of roads in Lebanon ####################################################################
nodes = pd.read_excel(properties.all_road_nodes)

# ######### Method 2: Using G function ###################################################################################
    # max_lat = max(data['location_latitude'])+5
    # min_lat = min(data['location_latitude'])-5
    # max_lng = max(data['location_longitude'])+5
    # min_lng = min(data['location_longitude'])-5

    # G = ox.graph_from_bbox(max_lat, min_lat, max_lng,  min_lng , network_type='drive')
    # # G = ox.graph_from_place('Lebanon', network_type='drive')

Read_Nodes = time.time()
time1 = Read_Nodes - start_time
print("Done Reading BTS data and Nodes data in: ",time1)

enter_loop = time.time()
time2 = enter_loop - start_time
print("Entered Loop Now: ",time2)

nodes_sector, closest_pt = pts_within_sec.get_nodes_within_sec_and_closest_pt(data, nodes)


data["nodes_in_sector"] = nodes_sector
data["closest_point"] = closest_pt

done = time.time()
time3 = done - start_time
print("Done All ",time3)

# Specify the file name and path where you want to save the Excel file
file_name = "/u01/jupyter-scripts/Pedro/CDR_trace/data/Generated Points within sector and closest point/final_excel.xlsx"

# Save the DataFrame to an Excel file
# If you want to exclude saving the index of the DataFrame, set index=False
data.to_excel(file_name, index=False, engine='openpyxl')
# data.to_csv(f"C:\\Users\\pedro.m\\Desktop\\CDR_vscode\\data\\final_excel.csv",index = False)

print(f"DataFrame saved to {file_name}")