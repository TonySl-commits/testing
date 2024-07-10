from vcis.utils.properties import CDR_Properties
import random
import string
import numpy as np
import pandas as pd
import math
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from scipy.spatial.distance import cdist
import os
from datetime import datetime
from shapely.geometry import Point, Polygon
import logging
import sys
import plotly.graph_objects as go
import folium
from datetime import datetime, timedelta
from cassandra.auth import PlainTextAuthProvider
import subprocess
import reverse_geocoder as rg
##############################################################################################################################

class CDR_Utils():

    def __init__(self, verbose: bool = False):
        self.properties = CDR_Properties()
        self.verbose = verbose
        self.passed_filepath = self.properties.passed_filepath_alfa
        # self.all_files = self.properties.all_files
        self.dataframes = []

##############################################################################################################################
    # Common Functions
##############################################################################################################################
    
    def calculate_box_boundaries(self,lat, lon, width_meters):
        # Approximate conversion of meters to degrees for latitude
        lat_delta = width_meters / 111111 # Approximately 111,111 meters per degree of latitude
        lon_delta = width_meters / (111111 * np.cos(np.radians(lat))) # Approximately 111,111 meters per degree of longitude
        return lat - lat_delta, lon - lon_delta, lat + lat_delta, lon + lon_delta
    
    def get_first_filename(self,directory):
        return os.listdir(directory)[0]
    
    def get_step_size(self,distance:int=200):
        latitude= 33.8
        one_degree_lat = 110.574
        one_degree_lat_radians = math.radians(latitude)
        one_degree_long = 111.320*math.cos(one_degree_lat_radians)
        step_lon = (distance / (one_degree_long*1000))
        step_lat = (distance / (one_degree_lat*1000))
        return step_lat,step_lon 

    def binning_lat(self,data,step):
        to_bin = lambda x: np.floor(x / step) * step
        data.loc[:, "latitude_grid"] = data['location_latitude'].apply(to_bin)
        data.loc[:, "latitude_grid"] = data['latitude_grid'] + step/2
        return data

    def binning_lon(self,data,step):
        to_bin = lambda x: np.floor(x / step) * step
        data.loc[:, "longitude_grid"] = data['location_longitude'].apply(to_bin)
        data.loc[:, "longitude_grid"] = data['longitude_grid']+ step/2
        return data

    def binning(self,data,distance:int=200,ret:bool=False):
        step_lat,step_lon = self.get_step_size(distance = distance)
        data = self.binning_lat(data,step_lat)
        data = self.binning_lon(data,step_lon)
        if ret:
            return data,step_lat,step_lon
        return data

    def combine_coordinates(self, data, longitude_column : str = 'longitude_grid', latitude_column : str = 'latitude_grid'):
        data.loc[:, 'grid'] = data[latitude_column].astype(str) + ','\
            + data[longitude_column].astype(str)
        return data
    
##############################################################################################################################
    # Reverse Geocoder
    ##############################################################################################################################
    def reverseGeocode(self,coordinates):
        result = rg.search(coordinates)
        return result
    
    def add_reverseGeocode_columns(self,df):
        df.to_csv(self.properties.passed_filepath_temp_geocode,index=False)
        subprocess.run(["python", self.properties.passed_filepath_exec_geocode])
        df = pd.read_csv(self.properties.passed_filepath_temp_geocode)
        # os.remove(self.properties.passed_filepath_temp_geocode)
        return df
    
    def print_dictionary(self, dictionary : dict, subject : str = None):
        if subject != None:
            print(f'\nINFO:    {subject.upper()}:')

        max_key_length = max(len(key) for key in dictionary.keys())
        for key, value in dictionary.items():
            print(f'        {key}'.ljust(max_key_length + 10), value)   

    
##############################################################################################################################
    # Cell Tower
    ##############################################################################################################################
    
    def truncate_and_divide(self,x):
        return math.trunc(x * 1000) / 1000
    
    def read_cdr_bts(self) -> pd.DataFrame:
        df = pd.read_excel(self.passed_filepath, sheet_name=['2G', '3G', 'LTE'], engine='openpyxl')
        df1, df2, df3 = list(df.values())
        return df1,df2,df3
    
    def read_cdr_records(self,all_files) -> pd.DataFrame:
        for file in all_files:
            df = pd.read_csv(file, delimiter=';')
            if self.properties.data_type_bf in df.columns:
                try:
                    df = df.drop(columns=[self.properties.data_type2_bf])
                except:
                    pass
            else:
                df=df.rename(columns={self.properties.data_type2_bf:self.properties.data_type_bf})
            if self.properties.start_site_name not in df.columns:
                df.rename(columns={self.properties.site_name_bf:self.properties.start_site_name}, inplace=True)
                df[self.properties.end_site_name] = df[self.properties.start_site_name]
            self.dataframes.append(df)

        df = pd.concat(self.dataframes, axis=0, ignore_index=True)
        df = df.sort_values(by= self.properties.start_timestamp)
        return df
    
    def read_csv_files(self,folder_path):
        dfs = []
        for file_name in os.listdir(folder_path):
            if file_name.endswith('.csv'):
                file_path = os.path.join(folder_path, file_name)
                try:
                    dff = pd.read_csv(file_path)
                    dfs.append(dff)
                except Exception as e:
                    print(f"Error reading file '{file_name}': {e}")

        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            return combined_df
        else:
            print("No CSV files found in the specified folder.")
            return None
    ##############################################################################################################################
    # Spark Connection
    ##############################################################################################################################
    
    # Function to connect to spark
    def get_spark_connection(self,passed_connection_host: str = "10.1.10.66" , passed_instances: int = 1 , passed_executor_memory: int = 5 , passed_driver_memory: int = 5):
        spark = SparkSession.builder.master('local[*]')\
        .config("spark.cassandra.connection.host", passed_connection_host )\
        .config("spark.executor.instances",f'{passed_instances}')\
        .config("spark.executor.memory",f'{passed_executor_memory}G')\
        .config("spark.driver.memory",f'{passed_driver_memory}G')\
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
        .getOrCreate()
        return spark
    
    # def get_spark_connection(self,passed_master_url: str = "spark://10.10.10.101:7077",
    #                          passed_connection_host: str = "10.1.10.66"  , 
    #                          passed_executor_memory: int = 1 , 
    #                          passed_driver_memory: int = 1 , 
    #                          passed_partitions_nb: int = 200 , 
    #                          passed_parallelism_nb: int = 10,
    #                          passed_cassandra_username = 'cassandra',
    #                          passed_cassandra_password = 'cassandra',
    #                          passed_AppName = 'CDR_Trace'
    #                          ):
    #     spark = SparkSession.builder.master(passed_master_url)\
    #     .config("spark.cassandra.connection.host",passed_connection_host)\
    #     .config("spark.executor.memory",f'{passed_executor_memory}G')\
    #     .config("spark.driver.memory",f'{passed_driver_memory}G')\
    #     .config("spark.sql.shuffle.partitions",f'{passed_partitions_nb}')\
    #     .config("spark.default.parallelism",f'{passed_parallelism_nb}')\
    #     .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
    #     .config("spark.sql.catalog.casscatalog", "com.datastax.spark.catalog.CatalogConnector") \
    #     .config("spark.cassandra.auth.username", passed_cassandra_username) \
    #     .config("spark.cassandra.auth.password", passed_cassandra_password) \
    #     .appName(passed_AppName)\
    #     .getOrCreate()
    #     return spark

##############################################################################################################################
    # Number Generation
######################################  GENERATE RANDOM IMSI IMEI NUMBER  ######################################
    def generate_random_phone_number(self):
        prefix = '961' + random.choice(['03', '70', '71', '76', '79', '81'])
        last_6_digits = ''.join(random.choices(string.digits, k=6))
        phone_number = int(prefix + last_6_digits)
        return phone_number

    def generate_random_imei(self):
        imei = random.choice(string.digits[1:]) + ''.join(random.choices(string.digits, k=13))
        total = sum(int(imei[i]) * (2 if i % 2 == 0 else 1) for i in range(14))
        check_digit = (10 - total % 10) % 10
        
        if check_digit == 0:
            check_digit += 1
        imei += str(check_digit)
        return imei

    def generate_random_location():
        return (random.uniform(-90, 90), random.uniform(-180, 180))

    def generate_random_imsi(self):
        mcc = '415'  
        mnc = ''.join(random.choices('0123456789', k=2))
        remaining_digits = 15 - len(mcc) - len(mnc)
        msin = ''.join(random.choices('0123456789', k=remaining_digits))
        imsi_str = f'{mcc}{mnc}{msin}'
        imsi = int(imsi_str)
        return imsi    
    def take_last_3(self,data):

        last_digits = str(int(data[self.properties.phone_number].head(1).values[0]))[-3:]
        print("Last-digits: ",last_digits)
        return str(last_digits)
    
    def replace_imsi_with_hash(self, data,last_digit:int = None):
        if last_digit == None:
            last_digit = self.take_last_3(data)
        print(last_digit)
        data[self.properties.imsi] = str(data[self.properties.imsi].head(1).values[0])[1:-2]+ last_digit
        print(data[self.properties.imsi])
        return data
    
    ##############################################################################################################################
    def create_polygon(self, lat, lon, azimuth, length=100, width=50):
        azimuth_rad = np.radians(azimuth)
        
        vertices = [
            (lon + length/2 * np.cos(azimuth_rad), lat + length/2 * np.sin(azimuth_rad)),
            (lon - width/2 * np.sin(azimuth_rad), lat + width/2 * np.cos(azimuth_rad)),
            (lon + width/2 * np.sin(azimuth_rad), lat - width/2 * np.cos(azimuth_rad))
        ]
        return Polygon(vertices)

    def get_polygon_vertices(self, data, distance_km:float = 0.4):
         
        def get_front_and_back_bts_sector_vertices(group):
            # If the left or right angle are null, do not compute the vertices
            if group['left_angle'].isna().any() | group['right_angle'].isna().any():
                group.loc[:, ['vertex1', 'vertex2', 'vertex3', 'vertex4']] = 'nan'
            else:
                latitude = group['location_latitude'].values.tolist()[0]
                longitude = group['location_longitude'].values.tolist()[0]
                left_angle = group['left_angle'].values.tolist()[0]
                right_angle = group['right_angle'].values.tolist()[0]
                
                lat1, long1 = self.calculate_coordinates(latitude, longitude, left_angle, distance_km)
                lat2, long2 = self.calculate_coordinates(latitude, longitude, right_angle, distance_km)

                opposite_left_angle = (360 + (left_angle - 180)) % 360
                opposite_right_angle = (360 + (right_angle + 180)) % 360
                
                lat3, long3 = self.calculate_coordinates(latitude, longitude, opposite_left_angle, distance_km = 0.05)
                lat4, long4 = self.calculate_coordinates(latitude, longitude, opposite_right_angle, distance_km = 0.05)

                group['vertex1'] = str((lat1, long1))
                group['vertex2'] = str((lat2, long2))
                group['vertex3'] = str((lat3, long3))
                group['vertex4'] = str((lat4, long4))
                
            return group
        data['location_latitude'] = data['location_latitude'].astype(float)
        data['location_longitude'] = data['location_longitude'].astype(float)
        data = data.groupby(['cgi_id']).apply(get_front_and_back_bts_sector_vertices)
        data = data.reset_index()

        return data
        
    def get_distance_to_polygon_base(self, device_location, polygon):
        point = Point(device_location[1], device_location[0])
        nearest_point = polygon.exterior.interpolate(polygon.exterior.project(point))
        return point.distance(nearest_point)

    def get_closest_bts(self, device_lat, device_lon, bts_table, num_closest=4, azimuth_threshold=8, max_distance=10):
        device_location = np.array([[device_lat, device_lon]])
        bts_locations = bts_table[['location_latitude', 'location_longitude', 'location_azimuth']].values

        distances = cdist(device_location, bts_locations[:, :2], metric='euclidean')[0]

        within_distance_indices = np.where(distances <= max_distance)[0]
        filtered_bts_locations = bts_locations[within_distance_indices]

        azimuth_diff = np.abs(filtered_bts_locations[:, 2] - azimuth_threshold)
        azimuth_valid_indices = np.where(azimuth_diff <= azimuth_threshold)[0]
        final_bts_locations = filtered_bts_locations[azimuth_valid_indices]

        sorted_indices = np.argsort(distances[within_distance_indices][azimuth_valid_indices])
        closest_bts_indices = sorted_indices[:num_closest]

        closest_bts_cgi_ids = bts_table.iloc[within_distance_indices[azimuth_valid_indices[closest_bts_indices]]]['cgi_id'].tolist()
        closest_bts_lat_lon_azimuth = bts_table[bts_table['cgi_id'].isin(closest_bts_cgi_ids)]

        for index, row in closest_bts_lat_lon_azimuth.iterrows():
            polygon = self.create_polygon(row['location_latitude'], row['location_longitude'], row['location_azimuth'])
            distance = self.get_distance_to_polygon_base([device_lat, device_lon], polygon)

            closest_bts_lat_lon_azimuth.at[index, 'distance_to_polygon_base'] = distance

        closest_bts_lat_lon_azimuth = closest_bts_lat_lon_azimuth[closest_bts_lat_lon_azimuth['distance_to_polygon_base'] <= max_distance]

        if closest_bts_lat_lon_azimuth.empty:
            return None  

        closest_bts = closest_bts_lat_lon_azimuth.loc[closest_bts_lat_lon_azimuth['distance_to_polygon_base'].idxmin()]
        selected_cgi_id = closest_bts['cgi_id']

        return selected_cgi_id
##############################################################################################################################
    # Distance Functions
    ##############################################################################################################################
    def haversine(self, lat1, lon1, lat2, lon2, radius=6371):
        radius = radius

        lat1 = math.radians(lat1)
        lon1 = math.radians(lon1)
        lat2 = math.radians(lat2)
        lon2 = math.radians(lon2)

        dlon = lon2 - lon1
        dlat = lat2 - lat1

        a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        distance = radius * c
        return distance
    

##############################################################################################################################
    # Sector Functions
    ##############################################################################################################################
   
    def calculate_coordinates(self, latitude, longitude, azimuth_deg , distance_km = 0.15 ,radius = 6371):
        earth_radius = radius

        azimuth_rad = math.radians(azimuth_deg)

        new_lat = math.degrees(math.asin(math.sin(math.radians(latitude)) * math.cos(distance_km / earth_radius) +
                                         math.cos(math.radians(latitude)) * math.sin(distance_km / earth_radius) *
                                         math.cos(azimuth_rad)))
        new_lon = longitude + math.degrees(math.atan2(math.sin(azimuth_rad) * math.sin(distance_km / earth_radius) * math.cos(math.radians(latitude)),
                                                           math.cos(distance_km / earth_radius) - math.sin(math.radians(latitude)) * math.sin(math.radians(new_lat))))
        return    new_lat,new_lon
    
    def calculate_sector_triangle(self , latitude, longitude, azimuth, distance_km:float = 0.15):
        azimuth1 = (azimuth - 30) % 360
        azimuth2 = (azimuth + 30) % 360

        vertex1 = self.calculate_coordinates(latitude, longitude, azimuth1 )
        vertex2 = self.calculate_coordinates(latitude, longitude, azimuth2 )

        return [
            list([latitude,longitude]),
            list(vertex1),
            list(vertex2)
        ]

    def find_circumcenter_radius(self, latitude, longitude, azimuth):
        vertices = self.calculate_sector_triangle(latitude, longitude, azimuth)
        if len(vertices) != 3:
            raise ValueError("The input list must contain exactly three vertices.")

        circumcenter_x = (vertices[0][0] + vertices[1][0] + vertices[2][0]) / 3
        circumcenter_y = (vertices[0][1] + vertices[1][1] + vertices[2][1]) / 3
        
        circumcenter = (circumcenter_x, circumcenter_y)
        radius = self.haversine(circumcenter[0], circumcenter[1], vertices[0][0], vertices[0][1])

        return circumcenter, radius

    ##############################################################################################################################
        
    def seconds_to_hhmmss(self,seconds):
        return str(timedelta(seconds=seconds))

    def convert_ms_to_datetime_value(self,variable:int):
        try:
            variable = pd.to_datetime(pd.to_numeric(variable), unit='ms')
        except Exception as e:
            if self.verbose == True:
                print(f'DateTime Convert skipped: {e}')
            pass
        return variable
    
    def convert_ms_to_datetime(self,df,column_name="usage_timeframe"):
        try:
            df[column_name] = pd.to_datetime(df[column_name], unit='ms')
        except Exception as e:
            if self.verbose == True:
                print(f'DateTime Convert skipped: {e}')
            pass
        return df
    
    def convert_datetime_to_ms(self,df, date_column_name:str='usage_timeframe',format = '%m/%d/%Y %H:%M:%S',offset = False) -> pd.DataFrame:
        try:
            df[date_column_name] = pd.to_datetime(df[date_column_name], format=format)
        except:
            pass
        try:
            df[date_column_name] = pd.to_datetime(df[date_column_name], format='%Y-%m-%d %H:%M:%S')
        except:
            pass
        try:
            df[date_column_name] = pd.to_datetime(df[date_column_name], format='%m/%d/%Y %H:%M:%S')
        except:
            pass
        try:
            df[date_column_name] = pd.to_datetime(df[date_column_name], format='%m/%d/%Y %H:%M')
        except:
            pass
        try:
            df[date_column_name] = pd.to_datetime(df[date_column_name], format='%Y-%m-%dT%H:%M:%S.%f')
        except Exception as e:
            if self.verbose == True:
                print(f'DateTime Convert skipped: {e}')
            pass
        if offset:
            df[date_column_name] = df[date_column_name].dt.tz_localize('EET')
        df[date_column_name] = df[date_column_name].astype(np.int64) // 10**6

        return df

    def convert_datetime_to_ms_str(self,date,format='%Y-%m-%d'):
        timestamp = int(datetime.strptime(date,format).timestamp() * 1000)
        return timestamp
    
##############################################################################################################################
    # Splitting for Cassandra Tables
##############################################################################################################################
   
    def get_month_year_combinations(self,start_date, end_date):
        if isinstance(start_date, (int, np.int64)):
            start_date = self.convert_ms_to_datetime_value(start_date)
        if isinstance(end_date, (int, np.int64)):
            end_date = self.convert_ms_to_datetime_value(end_date)
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
            
        month_year_combinations = []
        current_date = start_date

        while current_date <= end_date:
            month_year_combinations.append((current_date.year, current_date.month))
            current_date = (current_date + timedelta(days=31)).replace(day=1)

        return month_year_combinations 
    
    def get_month_year_combinations_u(self,df):
        df['date'] = pd.to_datetime(df['usage_timeframe'],unit = 'ms')
        df['m'] = df['date'].dt.month
        df['y'] = df['date'].dt.year
        dates = df[['m', 'y']].drop_duplicates()
        dates = dates.values.tolist()
        print(dates)
        return dates 

    def filter_data_on_month_year_combo(self, data: pd.DataFrame, month_year_combo: list):
        data_list = []
        # Convert usagetimeframe column to datetime
        data = self.convert_ms_to_datetime(data = data)

        for year, month in month_year_combo:
            # Filter DataFrame based on year and month
            filtered_data = data[(data['usage_timeframe'].dt.year == year) & (data['usage_timeframe'].dt.month == month)]
            print(len(filtered_data))
            # Append the filtered DataFrame to the list
            data_list.append(filtered_data)

        return data_list
    
    def dataframe_to_list_of_lists(self,df):
        # Convert the DataFrame to a list of lists, including column names
        list_of_lists = [df.columns.values.tolist()] + df.values.tolist()
        return list_of_lists

##############################################################################################################################
    # New df Visits
    ##############################################################################################################################
    def is_neighbor(self,grid1, grid2, step_lat=0.1, step_lon=0.1):
        lat1, lon1 = map(float, grid1.split(','))
        lat2, lon2 = map(float, grid2.split(','))
        return (abs(lat1 - lat2) <= step_lat) and (abs(lon1 - lon2) <= step_lon)
        
    def get_visit_df(self,df,t_diff:int=2,step_lat=0.01,step_lon=0.01):
        df = df.reset_index(drop=True)
        if df['usage_timeframe'].dtype == np.int64:
            df['usage_timeframe'] = pd.to_datetime(df['usage_timeframe'],unit='ms')
        x = 0
        df  = df.sort_values(['usage_timeframe'],ascending=False)
        df_visit = pd.DataFrame(columns=['device_id', 'start_time', 'end_time', 'location_latitude', 'location_longitude', 'grid'])
        i=1
        # Iterate over the DataFrame rows
        while True:
            if x + i < len(df):
                if df.loc[x, 'grid'] == df.loc[x + i, 'grid']:
                    # Calculate the time difference
                    time_diff = df.loc[x + i, 'usage_timeframe'] - df.loc[x+i-1, 'usage_timeframe']
                    # If the time difference is less than 5 hours, continue to the next iteration
                    if time_diff.total_seconds() < t_diff * 3600:
                        i=i+1
                        continue
                    # Otherwise, save the visit and update x
                    else:
                        new_row = pd.DataFrame({
                            'device_id': [df.loc[x, 'device_id']],
                            'start_time': [df.loc[x, 'usage_timeframe']],
                            'end_time': [df.loc[x + i, 'usage_timeframe']],
                            'location_latitude': [df.loc[x, 'location_latitude']],
                            'location_longitude': [df.loc[x, 'location_longitude']],
                            'grid': [df.loc[x, 'grid']]
                        })
                        df_visit = pd.concat([df_visit, new_row], ignore_index=True)
                        x = x + i 
                        i=1
                # If the grids are different, check if they are neighbors
                else:
                    if self.is_neighbor(df.loc[x, 'grid'], df.loc[x + i, 'grid'],step_lat,step_lon):
                        # Calculate the time difference
                        time_diff = df.loc[x + i, 'usage_timeframe'] - df.loc[x+i-1, 'usage_timeframe']
                        # If the time difference is less than 5 hours, continue to the next iteration
                        if time_diff.total_seconds() < t_diff * 3600:
                            i=i+1
                            continue
                        # Otherwise, save the visit and update x
                        else:
                            new_row = pd.DataFrame({
                                'device_id': [df.loc[x, 'device_id']],
                                'start_time': [df.loc[x, 'usage_timeframe']],
                                'end_time': [df.loc[x + i-1, 'usage_timeframe']],
                                'location_latitude': [df.loc[x, 'location_latitude']],
                                'location_longitude': [df.loc[x, 'location_longitude']],
                                'grid': [df.loc[x, 'grid']]
                            })
                            df_visit = pd.concat([df_visit, new_row], ignore_index=True)
                            x = x + i 
                            i=1
                    # If they are not neighbors, save the visit and update x
                    else:
                        new_row = pd.DataFrame({
                            'device_id': [df.loc[x, 'device_id']],
                            'start_time': [df.loc[x, 'usage_timeframe']],
                            'end_time': [df.loc[x + i-1, 'usage_timeframe']],
                            'location_latitude': [df.loc[x, 'location_latitude']],
                            'location_longitude': [df.loc[x, 'location_longitude']],
                            'grid': [df.loc[x, 'grid']]
                        })
                        df_visit = pd.concat([df_visit, new_row], ignore_index=True)
                        x = x + i 
                        i=1
                
            else:
                break

        # Handle the last row
        if x < len(df) - 1:
            new_row = pd.DataFrame({
                'device_id': [df.loc[x, 'device_id']],
                'start_time': [df.loc[x, 'usage_timeframe']],
                'end_time': [df.loc[x, 'usage_timeframe']],
                'location_latitude': [df.loc[x, 'location_latitude']],
                'location_longitude': [df.loc[x, 'location_longitude']],
                'grid': [df.loc[x, 'grid']]
            })
            df_visit = pd.concat([df_visit, new_row], ignore_index=True)

        df_visit['start_time'] = pd.to_datetime(df_visit['start_time'])
        df_visit['end_time'] = pd.to_datetime(df_visit['end_time'])

        return df_visit
    
##############################################################################################################################
    # Report General Functions
    ##############################################################################################################################
    def add_css_to_html_table(self,table):
        table = table.to_html(classes='table-style', escape=False,index=False)
        css_set_column_width = """
            <style>
                .table-style {
                border-collapse: collapse;
                width: 100%;
                }

                .table-style th, .table-style td {
                border: 1px solid #ddd;
                padding: 8px;
                text-align: center;
                color: #002147; /* Text color for table cells */
                font-family: 'Times New Roman', Times, serif; /* Font family for the entire table */
                font-size: 16px; /* Font size for the entire table */
                }

                .table-style th {
                background-color: #03045E;
                color: white;
                font-weight: bold; /* Font weight for table headers */
                }

                .table-style td {
                background-color: #f2f2f2; /* Background color for table cells */
                }

                .table-style {
                width: 100%;
                }

                /* .table-style th, .table-style td {
                width: 33%; */
                }
            </style>
            """
        summary_table = css_set_column_width + table

        return summary_table
        
    def get_simple_ids(self,table:pd.DataFrame=None):
        unique_device_ids = table['device_id'].unique()
        simplified_ids = {device_id: f'Device-{i:03d}' for i, device_id in enumerate(unique_device_ids, start=1)}
        mapping_table = pd.DataFrame(list(simplified_ids.items()), columns=['Original ID', 'Simplified ID'])
        # mapping_table = self.dataframe_to_list_of_lists(mapping_table)
        table['device_id'] = table['device_id'].map(simplified_ids)
        return table , mapping_table
    