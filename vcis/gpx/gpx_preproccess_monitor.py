import gpxpy
import folium
import pandas as pd
import os
from datetime import timedelta
import uuid
from pyspark.sql.types import LongType, IntegerType, StringType , DecimalType
import warnings
warnings.filterwarnings('ignore')

from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools

class GPXPreprocessMonitor:
    def __init__(self, folder_path:str = CDR_Properties().passed_filepath_gpx_folder, cassandra_write: bool= False  ):
        self.folder_path = folder_path
        self.time_list = []
        self.latitudes = []
        self.longitudes = []
        self.map = folium.Map()
        self.trajectory_id_number = 0
        self.cassandra_table_list = []
        self.cassandra_write = cassandra_write
        self.properties = CDR_Properties()
        self.utils = CDR_Utils()
        self.propeties = CDR_Properties()
        self.cassandra_spark_tools = CassandraSparkTools()


    def read_gpx_folder(self):
        for file_name in os.listdir(self.folder_path):
            if file_name.endswith(".gpx"):
                print(file_name)
                file_path = os.path.join(self.folder_path, file_name)
                with open(file_path, 'r') as gpx_file:
                    # Parse the GPX data
                    gpx = gpxpy.parse(gpx_file)
                    df_gpx = self.preprocess_gpx(gpx)
                    
        return df_gpx
    
    def preprocess_gpx(self,gpx):
        for track in gpx.tracks:
            for segment in track.segments:
                for point in segment.points:
                    self.time_list.append(point.time)
                    self.longitudes.append(point.longitude)
                    self.latitudes.append(point.latitude)

        df_gpx = pd.DataFrame({'usage_timeframe': self.time_list, 'location_longitude': self.longitudes, 'location_latitude': self.latitudes})
        return df_gpx 
    
    def process_gpx_folder(self , df_gpx:pd.DataFrame = None, device_id:str = None,offset_hours:int = 2,region:str="145",sub_region="142"):
        df_gpx = self.utils.convert_ms_to_datetime(df_gpx)
        df_gpx[self.properties.usage_timeframe] = df_gpx[self.properties.usage_timeframe]+ pd.Timedelta(hours=offset_hours)
        df_gpx['location_name'] = "000000000000000"
        df_gpx['service_provider_id'] = 9
        df_gpx['day_no'] = df_gpx['usage_timeframe'].dt.day
        df_gpx['country_alpha2'] = 'LB'
        df_gpx['country_code'] = 422
        df_gpx['region_code'] = region
        df_gpx['subregion_code'] = sub_region
        df_gpx['hour_no'] = df_gpx['usage_timeframe'].dt.hour
        df_gpx['month_no'] = df_gpx['usage_timeframe'].dt.month
        df_gpx['year_no'] = df_gpx['usage_timeframe'].dt.year
        df_gpx['source_file_id'] = 1
        
        df_gpx = self.utils.convert_datetime_to_ms(df_gpx)

        return df_gpx
    
    def plot_gpx_folium(self, df_gpx):
        for i in range(len(df_gpx)):
            latitude = df_gpx['location_latitude'][i]
            longitude = df_gpx['location_longitude'][i]
            popup = df_gpx['usage_timeframe'][i]

            # Add a marker for each point on the map
            popup_text = f"Time: {self.utils.convert_ms_to_datetime_value(popup)+ timedelta(hours=2) }"
            folium.Marker([latitude, longitude], popup=popup_text).add_to(self.map)
        return self.map

    
    def find_intervals(self, df_gpx, hours_diff = 2):

        milliseconds_diff = ((hours_diff*60)*60)*1000

        # Sorting the DataFrame by the 'usage_timeframe' column
        df_gpx = df_gpx.sort_values(by='usage_timeframe')  

        # Calculate the difference between consecutive rows
        df_gpx['time_diff'] = df_gpx["usage_timeframe"].diff()

        # Initialize variables
        intervals = []
        interval_start_index = 0  # Start of the current interval

        for i in range(1, len(df_gpx)):
            # Check if the time difference with the next row is more than 2 hours
            if df_gpx.loc[i, 'time_diff'] > milliseconds_diff:
                # End the current interval at the previous row
                interval_end_index = i - 1
                # Save the interval (using DataFrame indices)
                intervals.append(df_gpx.iloc[interval_start_index:interval_end_index + 1])

                # Start a new interval from the current row
                interval_start_index = i

        # Add the last interval if not already added
        if interval_start_index < len(df_gpx):
            intervals.append(df_gpx.iloc[interval_start_index:])

        return intervals
    
    def write_into_cassandra(self , df_gpx:pd.DataFrame = None , cassandra_table:str = "geo_data_2023_12_142_145" , cassandra_keyspace:str = "datacrowd", cassandra_host:str = "10.1.10.66", cassandra_port:str = "9042"):
        start_date = df_gpx['usage_timeframe'].min()
        end_date = df_gpx['usage_timeframe'].max()

        month_year_combinations = self.utils.get_month_year_combinations(start_date, end_date)
        df_gpx_list = self.utils.filter_data_on_month_year_combo(df_gpx, month_year_combinations)
        print(cassandra_host)
        spark = self.cassandra_spark_tools.get_spark_connection(passed_connection_host = cassandra_host)
        i = 0
        for combo in month_year_combinations:
            year = combo[0]
            month = combo[1]
            print(df_gpx_list[i])
            pd.DataFrame.iteritems = pd.DataFrame.items
            df_gpx_list[i]  = self.utils.convert_date_to_timestamp(df_gpx_list[i])
            df_spark = spark.createDataFrame(df_gpx_list[i])
            i+=1

            cassandra_table = f"geo_data_{year}_{month}_142_145"
            self.cassandra_table_list.append(cassandra_table)

            # df_spark = df_spark.withColumn("usage_timeframe", df_spark["usage_timeframe"].cast(IntegerType())) 
            df_spark = df_spark.withColumnRenamed("Longitude", "location_longitude") 
            df_spark = df_spark.withColumnRenamed("Latitude", "location_latitude") 
            df_spark = df_spark.withColumn("service_provider_id", df_spark["service_provider_id"].cast(IntegerType()))
            df_spark = df_spark.withColumn("day_no", df_spark["day_no"].cast(IntegerType()))
            df_spark = df_spark.withColumn("country_code", df_spark["country_code"].cast(StringType()))
            df_spark = df_spark.withColumn("hour_no", df_spark["hour_no"].cast(IntegerType()))
            df_spark = df_spark.withColumn("month_no", df_spark["month_no"].cast(IntegerType()))
            df_spark = df_spark.withColumn("year_no", df_spark["year_no"].cast(IntegerType()))
            df_spark = df_spark.withColumn("source_file_id", df_spark["source_file_id"].cast(IntegerType()))
            print("The data for the GPX files in the monitor folder is being written to Cassandra")
            print(cassandra_table)
            print(df_spark.show())
            logs = pd.read_csv(self.propeties.passed_filepath_gpx_log + 'logs.csv')
            logs = logs._append({'geo_id': df_spark.select("device_id").collect()[0][0], 'start_date': start_date, 'end_date': end_date ,'cassandra_table':self.cassandra_table_list}, ignore_index=True)
            logs.to_csv(self.propeties.passed_filepath_gpx_log + 'logs.csv', index=False)
            if self.cassandra_write:
                df_spark.write \
                    .format("org.apache.spark.sql.cassandra") \
                    .mode("append") \
                    .options(table=cassandra_table, keyspace=cassandra_keyspace, user="cassandra", password="cassandra") \
                    .save()
            print("The data for the GPX files in the monitor folder has been written to Cassandra under this Geo ID :" + df_spark.select("device_id").collect()[0][0])
        spark.stop()
        
        
    def write_into_cassandra_geo(self , df_gpx:pd.DataFrame = None , cassandra_table:str = "geo_data_2023_12_142_145" , cassandra_keyspace:str = "datacrowd", cassandra_host:str = "10.1.10.66", cassandra_port:str = "9042",region:str="145",sub_region="142"):
        start_date = df_gpx['usage_timeframe'].min()
        end_date = df_gpx['usage_timeframe'].max()

        month_year_combinations = self.utils.get_month_year_combinations(start_date, end_date)
        df_gpx_list = self.utils.filter_data_on_month_year_combo(df_gpx, month_year_combinations)
        spark = self.cassandra_spark_tools.get_spark_connection(passed_connection_host = cassandra_host)
        i = 0
        for combo in month_year_combinations:
            year = combo[0]
            month = combo[1]
            pd.DataFrame.iteritems = pd.DataFrame.items
            df_gpx_list[i]  = self.utils.convert_date_to_timestamp(df_gpx_list[i])
            df_spark = spark.createDataFrame(df_gpx_list[i])
            i+=1

            cassandra_table = f"geo_data_{year}_{month}_{region}_{sub_region}"
            self.cassandra_table_list.append(cassandra_table)

            # df_spark = df_spark.withColumn("usage_timeframe", df_spark["usage_timeframe"].cast(IntegerType())) 
            df_spark = df_spark.withColumn("service_provider_id", df_spark["service_provider_id"].cast(IntegerType()))
            df_spark = df_spark.withColumn("day_no", df_spark["day_no"].cast(IntegerType()))
            df_spark = df_spark.withColumn("country_code", df_spark["country_code"].cast(StringType()))
            df_spark = df_spark.withColumn("hour_no", df_spark["hour_no"].cast(IntegerType()))
            df_spark = df_spark.withColumn("month_no", df_spark["month_no"].cast(IntegerType()))
            df_spark = df_spark.withColumn("year_no", df_spark["year_no"].cast(IntegerType()))
            df_spark = df_spark.withColumn("source_file_id", df_spark["source_file_id"].cast(IntegerType()))
            print("The data for the GPX files in the monitor folder is being written to Cassandra")
            print(cassandra_table)
            print(df_spark.show())

            if self.cassandra_write:
                try:
                    df_spark.write \
                        .format("org.apache.spark.sql.cassandra") \
                        .mode("append") \
                        .options(table=cassandra_table, keyspace=cassandra_keyspace, user="cassandra", password="cassandra") \
                        .save()
                    print("The data for the GPX files in the monitor folder has been written to Cassandra under this Geo ID :" + df_spark.select("device_id").collect()[0][0])
                except:
                    print("No table named :" + cassandra_table )
        spark.stop()
        