from vcis.aoi_classification.geolocation_data_analyzer import GeoLocationAnalyzer
from vcis.correlation.cdr_correlation_functions import CDRCorrelationFunctions
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.utils.utils import CDR_Properties
from vcis.utils.utils import CDR_Utils
import math
import pandas as pd

class AOI_Correlation:
    def __init__(self, geolocation_analyzer : GeoLocationAnalyzer):
        self.geolocation_analyzer = geolocation_analyzer

        self.correlation_functions = CDRCorrelationFunctions()
        self.cassandra_tools = CassandraTools()
        self.utils = CDR_Utils()
        self.properties = CDR_Properties()

        self.df_dicts = []
        self.df_lists = []


    def get_df_after_activity_scan_for_all_devices(self, start_date, end_date, passed_server):
        for device in range(self.geolocation_analyzer.geolocation_data_list.get_length()):
            geolocation_data = self.geolocation_analyzer.geolocation_data_list[device]
            
            final, dict = self.perform_activity_scan(geolocation_data, start_date, end_date, passed_server)
            
            self.df_dicts.append(dict)
            self.df_lists.append(final)

        final_list = pd.concat(self.df_lists)

        return final_list, self.df_dicts


    def perform_activity_scan(self, geolocation_data, start_date, end_date, passed_server : str = '10.1.10.110', default_timeout : int = 10000, default_fetch_size : int = 10000):
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
    
    def perform_activity_scan_for_all_devices(self, start_date, end_date, passed_server):
        final_df, list_dict = self.get_df_after_activity_scan_for_all_devices(start_date, end_date, passed_server)

        print(list_dict)

        for i, device_dict in enumerate(list_dict):
            device_id = list(device_dict.keys())[0]

            # Retrieve the geolocation_data for the current device_id
            geolocation_data = self.geolocation_analyzer.geolocation_data_list.get_geolocation_data(device_id).data
            device_df = device_dict[device_id]  # Get the DataFrame

            # Convert timestamps to datetime
            device_df['usage_timeframe'] = pd.to_datetime(device_df['usage_timeframe'])
            geolocation_data['Timestamp'] = pd.to_datetime(geolocation_data['Timestamp'])

            print("DONE NHAWWEL TO DATE-TIME")

            # Calculate time differences between each 'usage_timeframe' and 'Timestamp'
            # This creates a DataFrame of all combinations of differences
            time_diffs = device_df['usage_timeframe'].apply(lambda x: abs(geolocation_data['Timestamp'] - x))

            print("DONE time_diffs")

            # Find the minimum time difference for each 'usage_timeframe'
            min_time_diffs = time_diffs.min(axis=1)

            # Filter rows where the minimum time difference is within 15 minutes
            filtered_indices = min_time_diffs[min_time_diffs <= pd.Timedelta(minutes=15)].index
            filtered_df = device_df.loc[filtered_indices]

            print("DONE filtered_df")

            # Replace the original DataFrame in the list_dict with the filtered DataFrame
            list_dict[i][device_id] = filtered_df.reset_index(drop=True)
        
        print("AFTEERRRRRRRRRRRRRRRRR list_dict")
        print(list_dict)

        return final_df, list_dict

    
