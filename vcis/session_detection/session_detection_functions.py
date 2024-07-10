import pandas as pd
from datetime import datetime
from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
import numpy as np
import fastdtw
import time
import warnings
import math
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import timedelta

warnings.filterwarnings('ignore')
from geopy.distance import geodesic, great_circle
from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.plots.cotraveler_plots import CotravelerPlots
##################################################################################################################

class SessionDetectionFunctions():
    def __init__(self, verbose: bool = False):

        self.properties = CDR_Properties()
        self.utils = CDR_Utils(verbose=verbose)
        self.cassandra_tools = CassandraTools(verbose=verbose)
        self.session = self.cassandra_tools.get_cassandra_connection(server='10.1.10.110')
        
        
        pd.set_option('display.max_columns', 100)

   
    # def calculate_sectors_within_radius_single_point(self,latitude, longitude, radius, df):
    #     """
    #     Calculate the number of sectors within a given radius around a single point.
        
    #     Parameters:
    #     - latitude: float, latitude of the point
    #     - longitude: float, longitude of the point
    #     - radius: float, the radius in kilometers
    #     - df: pandas DataFrame containing 'location_latitude' and 'location_longitude' columns
        
    #     Returns:
    #     - int, number of sectors within the specified radius around the given point
    #     """
    #     sector_coordinates = np.column_stack((df['location_latitude'], df['location_longitude']))
    #     distances = np.array([geodesic((latitude, longitude), coord).kilometers for coord in sector_coordinates])
    #     is_within_radius = distances <= radius  # Boolean array indicating if each sector is within radius
    #     num_sectors_within_radius = np.sum(is_within_radius) - 1  # Exclude the sector itself
    #     return num_sectors_within_radius

    # def calculate_sectors_within_radius(self,df, radius):
    #     """
    #     Calculate the number of sectors within a given radius around each sector in the DataFrame.
        
    #     Parameters:
    #     - df: pandas DataFrame containing 'location_latitude' and 'location_longitude' columns
    #     - radius: float, the radius in kilometers
        
    #     Returns:
    #     - pandas Series, number of sectors within the specified radius for each row in df
    #     """
        
    #     def calculate_sectors(row):
    #         return self.calculate_sectors_within_radius_single_point(row['location_latitude'], row['location_longitude'], radius, df)
        
    #     # Apply the function to each row in the DataFrame
    #     num_sectors_within_radius = df.apply(calculate_sectors, axis=1)
    #     # Assign the result to a new column in the DataFrame
    #     df["number_of_sectors"] = num_sectors_within_radius
        
    #     # Return the calculated Series
    #     return num_sectors_within_radius

    
    def get_common_ids(self,df, size=2000):
        def get_ids_within_threshold(df, point_value, threshold, axis='location_latitude'):
            # point_value = df.loc[df['cgi_id'] == point_id, axis].values[0]
            lower_bound = point_value - threshold
            upper_bound = point_value + threshold
            ids_within_threshold = df[(df[axis] >= lower_bound) & (df[axis] <= upper_bound)]['cgi_id'].tolist()
            return ids_within_threshold

        step_lat, step_lon = self.utils.get_step_size(size)
        # df_sorted_x = df.sort_values(by='location_latitude').copy()
        # df_sorted_y = df.sort_values(by='location_longitude').copy()
        unique_coords = df[['location_latitude', 'location_longitude']].drop_duplicates()
        neighbor_data = []

        for _, row in unique_coords.iterrows():
            x, y = row['location_latitude'], row['location_longitude']
            ids_within_threshold_x = get_ids_within_threshold(df, x, step_lat, axis='location_latitude')
            ids_within_threshold_y = get_ids_within_threshold(df, y, step_lon, axis='location_longitude')
            
            common_ids = list(set(ids_within_threshold_x) & set(ids_within_threshold_y))
            cgi_id = df[(df['location_latitude'] == x) & (df['location_longitude'] == y)]['cgi_id'].unique().tolist()
            difference_ids = list(set(common_ids) - set(cgi_id))
            neighbor_data.append({'location_latitude':x,'location_longitude':y,'neighbor_ids': difference_ids,'number_of_sectors':len(difference_ids)})
            # print("CGI ID:  ",cgi_id,"-----------","Common ids: ",common_ids, "-----", "difference_ids: ", difference_ids)

        neighbor_df = pd.DataFrame(neighbor_data)
        return neighbor_df

    def determine_correct_cgi_subset(self,df, start_idx=0, end_idx=None):
        if end_idx is None:
            end_idx = len(df) - 1

        if start_idx<len(df)-1 and df.at[start_idx, 'cgi_id'] != df.at[start_idx + 1, 'cgi_id']:
            switching_pattern = {df.at[start_idx ,'cgi_id']:start_idx, df.at[start_idx+1, 'cgi_id']:start_idx+1 }
        else:
            return df ,start_idx
        # Identify the switching pattern
        switch_end_idx= start_idx+1
        for i in range(start_idx+2, end_idx-1):
            if i + 1 < len(df) and df.at[i, 'cgi_id'] != df.at[i + 1, 'cgi_id']:
                # print(df.at[i, 'cgi_id'],"---", df.at[i + 1, 'cgi_id'])
                if (df.at[i,"cgi_id"] in switching_pattern.keys()) and (df.at[i+1,"cgi_id"] in switching_pattern.keys()):
                    
                    switching_pattern[df.at[i ,'cgi_id']] = i
                    switching_pattern[df.at[i+1 ,'cgi_id']] = i+1
                    switch_end_idx = i+1
                else:
                
                    break
            else:
                break
        
        if not switching_pattern or switch_end_idx == start_idx+1:
                return df ,start_idx
            
        # print(switching_pattern)

        cgi_set = switching_pattern.keys()

        
        # if len(cgi_set) != 2:
        #     return df, end_idx
        
        cgi1, cgi2 = list(cgi_set)
        # print(cgi1,"_____________",cgi2)
        next_unique_cgis = set()
        for i in range(switch_end_idx + 1, len(df)):
            if df.at[i, 'cgi_id'] not in cgi_set:
                next_unique_cgis.add(df.at[i, 'cgi_id'])
            else:
                continue
            if len(next_unique_cgis) == 4:
                break
        
        # if len(next_unique_cgis) < 2:
        #     return df, end_idx
        
        next_unique_cgis = list(next_unique_cgis)
        
        def calculate_distance(cgi_id, next_cgi_id):
            row_cgi = df[df['cgi_id'] == cgi_id].iloc[0]
            row_next_cgi = df[df['cgi_id'] == next_cgi_id].iloc[0]
            return geodesic(
                (row_cgi['location_latitude'], row_cgi['location_longitude']),
                (row_next_cgi['location_latitude'], row_next_cgi['location_longitude'])
            ).kilometers
        
        # Calculate average distance for cgi1
        distances_cgi1 = [calculate_distance(cgi1, next_cgi) for next_cgi in next_unique_cgis]
        if distances_cgi1:
            avg_distance_cgi1 = sum(distances_cgi1) / len(distances_cgi1)
        else:
            avg_distance_cgi1 = 0  # handle case where no distances are calculated

        # Calculate average distance for cgi2
        distances_cgi2 = [calculate_distance(cgi2, next_cgi) for next_cgi in next_unique_cgis]
        if distances_cgi2:
            avg_distance_cgi2 = sum(distances_cgi2) / len(distances_cgi2)
        else:
            avg_distance_cgi2 = 0  # handle case where no distances are calculated
        correct_cgi = cgi1 if avg_distance_cgi1 < avg_distance_cgi2 else cgi2
        overshoot_cgi = cgi2 if correct_cgi == cgi1 else cgi1
        
        df.loc[start_idx:switch_end_idx, 'switching_overshoot'] = (df.loc[start_idx:switch_end_idx, 'cgi_id'] == overshoot_cgi)
        
        return df, switch_end_idx

    def flag_overshoots_based_on_previous_sectors(self,df,  sector_threshold, time_threshold, num_sectors_column=None,radius=2000, start_idx=1):
        
        if num_sectors_column is not None:
            num_sectors_within_radius = df[num_sectors_column]
        elif 'number_of_sectors'  in df.columns:
            num_sectors_within_radius = df["number_of_sectors"]
        else:
            # num_sectors_within_radius = calculate_sectors_within_radius(df, radius)
            num__of_sectors = self.get_common_ids(df,radius)
            temp = df.merge(num__of_sectors, on=['location_latitude', 'location_longitude'], how='inner').sort_values("START_DATE").reset_index(drop=True)
            num_sectors_within_radius= temp["number_of_sectors"]
            df["number_of_sectors"]= num_sectors_within_radius  
        
        df.loc[0,"potential_overshoot"] = False
        df.loc[0,"switching_overshoot"] = False

        # print(num_sectors_within_radius)


        
    # while start_idx < len(df):
        # print("loop 1")
        for i in range(1, len(df)):
            # start_idx = i
            j = i - 1
            while j >= 0 and df.at[j, 'potential_overshoot']:
                j -= 1
            distance = geodesic(
                (df.at[i, 'location_latitude'], df.at[i, 'location_longitude']),
                (df.at[j, 'location_latitude'], df.at[j, 'location_longitude'])
            ).meters
            time_diff = (pd.to_datetime(df.at[i, 'START_DATE']) - pd.to_datetime(df.at[j, 'START_DATE'])).total_seconds() / 60
            if distance < radius:
                df.at[i, 'potential_overshoot'] = False    
            elif time_diff > time_threshold:
                df.at[i, 'potential_overshoot'] = False
                ## the statements below are related to switching 
                # df, switch_end_idx = determine_correct_cgi_subset(df, start_idx=i)
                # if switch_end_idx == i:
                #     df.at[i, 'switching_overshoot'] = False
                # else:
                #     start_idx = switch_end_idx 
                #     break
            else:
                df.at[i, 'potential_overshoot'] = num_sectors_within_radius.iloc[j] >= sector_threshold
        # df.loc[start_idx:, "potential_overshoot"] = overshoot_flags[start_idx:]
        # start_idx += 1

        
        # print("loop2 ") 
        while start_idx < len(df):
            # print("start index: ",start_idx)
            for i in range(start_idx, len(df)):
                start_idx = i
                j = i - 1
                while j >= 0 and df.at[j, 'potential_overshoot']:
                    j -= 1
                # print("i: ",i,"-----------","j:",j)
                
                distance = geodesic(
                    (df.at[i, 'location_latitude'], df.at[i, 'location_longitude']),
                    (df.at[j, 'location_latitude'], df.at[j, 'location_longitude'])
                ).meters
            # if i!=1:
            #     time_diff = (pd.to_datetime(df.at[i, 'START_DATE']) - pd.to_datetime(df.at[j, 'START_DATE'])).total_seconds() / 3600.0
                    
            # else: 
            #     time_diff = math.inf  
            
                if distance < radius:
                    break
            # if time_diff > time_threshold:
                else:
                    df, switch_end_idx = self.determine_correct_cgi_subset(df, start_idx=i)
                    if switch_end_idx == i:
                        df.at[i, 'switching_overshoot'] = False
                    else:
                        start_idx = switch_end_idx 
                        break     
            start_idx += 1
            df.loc[df['potential_overshoot'] == False, 'switching_overshoot'] = False

            
        return df
        
        
        
        

    def calculate_time_spent_so_far(self,df):
        # df = df.sort_values(by='START_DATE').reset_index(drop=True)
        df['time_spent_so_far'] = 0
        if "START_DATE" in df.columns:
            df['START_DATE'] = pd.to_datetime(df["START_DATE"])
            previous_cgi_id =df.loc[0, 'cgi_id']
            accumulated_time = 0

            for i in range(1, len(df)):
                current_cgi_id = df.loc[i, 'cgi_id']
                previous_start_date = df.loc[i-1, 'START_DATE']
                current_start_date = df.loc[i, 'START_DATE']
                
                # print(f"Processing row {i}:")
                # print(f"  Previous CGI ID: {previous_cgi_id}, Current CGI ID: {current_cgi_id}")
                # print(f"  Previous Start Date: {previous_start_date}, Current Start Date: {current_start_date}")
                
                if current_cgi_id == previous_cgi_id:
                    time_diff = current_start_date - previous_start_date
                    accumulated_time += time_diff
                    # print(f"  Accumulated Time: {accumulated_time}")
                else:
                    accumulated_time = 0
                    # print("  Resetting accumulated time.")

                df.loc[i, 'time_spent_so_far'] = accumulated_time
                # print(f"  Time Spent So Far (seconds): {df.loc[i, 'time_spent_so_far']}")
                
                previous_cgi_id = current_cgi_id
                

            return df
        else:    
            previous_cgi_id =df.loc[0, 'cgi_id']
            accumulated_time = 0

            for i in range(1, len(df)):
                current_cgi_id = df.loc[i, 'cgi_id']
                previous_start_date = df.loc[i-1, 'usage_timeframe']
                current_start_date = df.loc[i, 'usage_timeframe']
                
                # print(f"Processing row {i}:")
                # print(f"  Previous CGI ID: {previous_cgi_id}, Current CGI ID: {current_cgi_id}")
                # print(f"  Previous Start Date: {previous_start_date}, Current Start Date: {current_start_date}")
                
                if current_cgi_id == previous_cgi_id:
                    time_diff = current_start_date - previous_start_date
                    accumulated_time += time_diff
                    # print(f"  Accumulated Time: {accumulated_time}")
                else:
                    accumulated_time = 0
                    # print("  Resetting accumulated time.")

                df.loc[i, 'time_spent_so_far'] = accumulated_time
                # print(f"  Time Spent So Far (seconds): {df.loc[i, 'time_spent_so_far']}")
                
                previous_cgi_id = current_cgi_id
                

            return df
        
    def generate_time_spent_flag(self,df, threshold_minutes):
        df = self.calculate_time_spent_so_far(df)
        threshold_seconds = threshold_minutes * 60  # Convert minutes to seconds
        df['time_spent_flag'] = df['time_spent_so_far'] > threshold_seconds
        return df


    def generate_visited_flag(self,df, time_window):
        df['potential_session_end'] = False
        
        # Dictionary to keep track of the last visit time for each cgi_id
        last_visit_time = {}
        
        for index, row in df.iterrows():
            current_time = row['END_DATE']
            
            if row['potential_overshoot'] == False:
                df.at[index, 'potential_session_end'] = None
            else:
                cgi_id = row['cgi_id']
                
                if cgi_id in last_visit_time:
                    last_time = last_visit_time[cgi_id]
                    time_diff = current_time - last_time
                    
                    if time_diff >= timedelta(minutes=time_window):
                        df.at[index, 'potential_session_end'] = True
                    # else:
                    #     df.at[index, 'potential_session_end'] = False
                else:
                    df.at[index, 'potential_session_end'] = False
                
                # Update the last visit time for this cgi_id
                last_visit_time[cgi_id] = current_time
        
        return df
    def generate_duration_at_bts_flag(self,df, threshold_minutes):
        
        df['potential_newly_active_location'] = ((df['Duration(s)'] > (threshold_minutes * 60)) & (df['Duration(s)'] < (24*3600))& df["potential_overshoot"])
        return df   
    
    
    def get_visits_df_from_CDR_Trace(self,df):
        df['usage_timeframe'] = pd.to_datetime(df['usage_timeframe'], unit='ms')

        # Find consecutive sessions based on changes in cgi_id
        df['session_id'] = (df['cgi_id'] != df['cgi_id'].shift(1)).cumsum()

        # Group by session_id and aggregate
        result_df = df.groupby('session_id').agg({
            # 'session_id':'first',
            'imsi_id': 'first',
            'cgi_id': 'first',
            'imei_id': 'first',
            'location_azimuth': 'first',
            'location_latitude': 'first',
            'location_longitude': 'first',
            'usage_timeframe': ['min', 'max', lambda x: (x.max() - x.min()).total_seconds()]
        }).reset_index(drop=True)

        # Flatten multi-index columns
        result_df.columns = ['imsi_id', 'cgi_id', 'imei_id', 'location_azimuth', 'location_latitude', 
                            'location_longitude', 'START_DATE', 'END_DATE', 'Duration(s)']
        result_df= result_df.sort_values("START_DATE")
        return result_df
