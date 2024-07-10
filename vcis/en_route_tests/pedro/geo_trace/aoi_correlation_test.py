from cdr_trace.aoi_classification.geolocation_data_analyzer import GeoLocationAnalyzer
from cdr_trace.correlation.correlation_functions import CorrelationFunctions
from cdr_trace.databases.cassandra.cassandra_tools import CassandraTools
from cdr_trace.utils.utils import CDR_Properties
from cdr_trace.utils.utils import CDR_Utils

import pandas as pd
from datetime import timedelta

class AOI_Correlation:
    def __init__(self, geolocation_analyzer : GeoLocationAnalyzer):
        self.geolocation_analyzer = geolocation_analyzer

        self.correlation_functions = CorrelationFunctions()
        self.cassandra_tools = CassandraTools()
        self.utils = CDR_Utils()
        self.properties = CDR_Properties()
        self.device_time_ranges = {}


    def get_df_after_filtering_for_all_devices(self):

        for device in range(self.geolocation_analyzer.geolocation_data_list.get_length()):
            geolocation_data = self.geolocation_analyzer.geolocation_data_list[device]
            data_aoi = geolocation_data.result_AOI
            data = geolocation_data.data

            # print("INFO:         NO OUTLIERS     ", geolocation_data.no_outliers)
            # print("INFO:         NO OUTLIERS     ", geolocation_data.no_outliers.columns)
            print("INFO:   cluster_label      NO OUTLIERS     ", geolocation_data.no_outliers['cluster_label'])
            print("INFO:   cluster_label_centroid      NO OUTLIERS     ", geolocation_data.no_outliers['cluster_label_centroid'].drop_duplicates())

            # print("DATAAAAAAAAAAAAAAAA AOIIIIIIIIIIIII",data_aoi)
            # print("DATAAAAAAAAAAAAAAAAA AAOIIIIIIIIIIIII",data_aoi.columns)

            # print("DATAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",data)
            # print("DATAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",data.columns)

            aoi_list = list(data_aoi[['NAME_ID', 'LAT', 'LNG']].drop_duplicates().itertuples(index=False, name=None)) 

            if not data.empty:
                # Calculate start and end times for each timestamp
                start_time = self.utils.timestamp_to_milliseconds(str(data['Timestamp'].min() - timedelta(minutes=15)))
                end_time = self.utils.timestamp_to_milliseconds(str(data['Timestamp'].max() + timedelta(minutes=15)))

                device_id = data_aoi['NAME_ID'].unique()[0]  

                self.device_time_ranges[device_id] = [start_time, end_time, aoi_list]
                
        print("NEEWWW device_time_rangesssssssssssssssssssss", self.device_time_ranges)

        return self.device_time_ranges


    def perform_activity_scan(self, dictionary, passed_server : str = '10.1.10.110', default_timeout : int = 10000, default_fetch_size : int = 10000):
        
        session = self.cassandra_tools.get_cassandra_connection(passed_server,
                                                default_timeout=default_timeout,
                                                default_fetch_size=default_fetch_size)
        
        data = geolocation_data.result_AOI

        df_list = []
        df = pd.DataFrame()

        # Create a list of tuples for each unique combination of LAT and LNG
        aoi_list = list(data[['NAME_ID', 'LAT', 'LNG']].drop_duplicates().itertuples(index=False, name=None))

        query = self.properties.activity_scan_circular_test

        year_month_combinations  = self.utils.get_month_year_combinations(start_date, end_date)

        for year, month in year_month_combinations:
            for device_id, latitude, longitude in aoi_list:
                query = self.correlation_functions.device_scan_month_year_query_builder(start_date = start_date,
                                                    end_date = end_date,
                                                    year = year,
                                                    month = month,
                                                    scan_distance = 200,
                                                    latitude = latitude,
                                                    longitude = longitude)

                try:
                    df = session.execute(query)
                    df = pd.DataFrame(df.current_rows)

                except Exception as e:
                    pass
                    # print("Exception raised:")
                    # print(e)

                if not df.empty:
                    df = df[['device_id','location_latitude','location_longitude','usage_timeframe']].drop_duplicates()                  
                    
                    df['DEVICE_MATCH'] = device_id  
                    
                    df_list.append(df)

        final = pd.concat(df_list)
        dict = {aoi_list[0][0] : final}

        return final, dict
    
    def perform_activity_scan_for_all_devices(self):

        dictionary = self.get_df_after_filtering_for_all_devices()
            
        final_df, list_dict = self.perform_activity_scan(dictionary = dictionary)
        
        # print("BEFOREEEEEEEEEEEEEEEEEEEEEEEEEEEEE list_dict")
        # print(list_dict)

        # for i, device_dict in enumerate(list_dict):
        #     device_id = list(device_dict.keys())[0]
            
        #     # Retrieve the geolocation_data for the current device_id
        #     geolocation_data = self.geolocation_analyzer.geolocation_data_list.get_geolocation_data(device_id).data
        #     device_df = device_dict[device_id]  # Get the DataFrame

        #     # Convert timestamps to datetime
        #     device_df['usage_timeframe'] = pd.to_datetime(device_df['usage_timeframe'])
        #     geolocation_data['Timestamp'] = pd.to_datetime(geolocation_data['Timestamp'])

        #     print("DONE NHAWWEL TO DATE-TIME")

        #     # Calculate time differences between each 'usage_timeframe' and 'Timestamp'
        #     # This creates a DataFrame of all combinations of differences
        #     time_diffs = device_df['usage_timeframe'].apply(lambda x: abs(geolocation_data['Timestamp'] - x))

        #     print("DONE time_diffs")

        #     # Find the minimum time difference for each 'usage_timeframe'
        #     min_time_diffs = time_diffs.min(axis=1)

        #     # Filter rows where the minimum time difference is within 15 minutes
        #     filtered_indices = min_time_diffs[min_time_diffs <= pd.Timedelta(minutes=15)].index
        #     filtered_df = device_df.loc[filtered_indices]

        #     print("DONE filtered_df")

        #     # Replace the original DataFrame in the list_dict with the filtered DataFrame
        #     list_dict[i][device_id] = filtered_df.reset_index(drop=True)
        
        # print("AFTEERRRRRRRRRRRRRRRRR list_dict")
        # print(list_dict)

        # return final_df, list_dict

    
