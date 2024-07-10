import numpy as np
import pandas as pd
import geopy.distance
import networkx as nx
import matplotlib.pyplot as plt
from shapely.geometry import Point, Polygon
import math
import folium
import time
import warnings
from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.trajectory.support_trajectory_functions import possible_points
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql import DataFrame as SparkDataFrame

# Filter out FutureWarnings
warnings.simplefilter(action='ignore', category=FutureWarning)

start_time = time.time()

class Points_within_sector():

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.utils = CDR_Utils()
        self.properties = CDR_Properties()
        self.nodes_sector = []
        self.closest_pt = []
        self.latitude_list = []
        self.longitude_list = []
        self.azimuth_list = []
        self.count_row = 0
        self.count_na = 0

    def put_data_to_process(self, data):
        # If "data" was a Spark Dataframe
        if isinstance(data, SparkDataFrame):
            data = data.toPandas()
        
        data = data.dropna(axis=0)
        data = data.drop_duplicates()
        data.reset_index(drop=True,inplace = True)
        data['location_azimuth'] = data['location_azimuth'].replace(to_replace=r'(?i)indoor', value=0, regex=True).astype(float)
        data['location_latitude'] = data['location_latitude'].astype(float)
        data['location_longitude'] = data['location_longitude'].astype(float)
        print(data.shape)
        
        return data

    def get_nodes_within_sec_and_closest_pt(self, data, nodes_dataset, save_split:int = 200, sector_width = 120, distance = 500):

        for _, row in data.iterrows():
            print ("Count: ",self.count_row)
            print ("Count Nulls: ",self.count_na)

            latitude = row['location_latitude']
            longitude = row['location_longitude']
            azimuth=row['location_azimuth']

            start_point = (latitude, longitude)
            sss = time.time()
            in_sector , closest = possible_points(nodes_dataset, start_point, azimuth, sector_width, distance)
            print("-"*10,"Done function.","-"*10)
            qqqqq = time.time() 
            x = qqqqq - sss
            print("Loop time: ",x)

            listt = []
            
            for _, row in in_sector.iterrows():
                coord = (row["y"],row["x"])
                listt.append(coord)

            if closest == None:
                self.count_na = self.count_na+1
            # else:
            self.closest_pt.append(closest)
            self.nodes_sector.append(listt)
            self.latitude_list.append(latitude)
            self.longitude_list.append(longitude)
            self.azimuth_list.append(azimuth)
            self.count_row = self.count_row+1

            if self.count_row % save_split == 0:
                df_chunck = pd.DataFrame()
                df_chunck["location_latitude"] = self.latitude_list
                df_chunck["location_longitude"] = self.longitude_list
                df_chunck["location_azimuth"] = self.azimuth_list
                df_chunck["nodes_in_sector"] = self.nodes_sector
                df_chunck["closest_point"] = self.closest_pt
                df_chunck.to_excel(self.properties.passed_filepath_generated_points_within_sector + f"{self.count_row}.csv",index = False, engine='openpyxl')
                print(f"DataFrame saved to {self.count_row}.xlsx")
        
        return self.nodes_sector, self.closest_pt
