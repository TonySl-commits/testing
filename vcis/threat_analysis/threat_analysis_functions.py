from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.utils.utils import CDR_Properties
from vcis.utils.utils import CDR_Utils

import pandas as pd
from geopy.distance import geodesic
import random
import numpy as np
from datetime import datetime

pd.options.mode.chained_assignment = None # default='warn'


class ThreatAnalysisFunctions():
    def __init__(self,verbose=False):
        self.cassandra_tools = CassandraTools(verbose=verbose)
        self.properties = CDR_Properties()
        self.utils = CDR_Utils(verbose=verbose)
        self.oracle_tools = OracleTools(verbose=verbose)      
        self.verbose = verbose

    def get_sus_visits(self, df_device, sus_areas):
        if 'country' not in df_device.columns:
            grouped_geofencing = self.utils.add_reverseGeocode_columns(df_device)
        else:
            grouped_geofencing = df_device.copy()
        grouped_geofencing.sort_values(by=['usage_timeframe'], ascending=True, inplace=True)
        grouped_geofencing = grouped_geofencing[['device_id','usage_timeframe','country','provinance', 'city']]

        def check_sus_area(row):
            for col in ['country', 'provinance', 'city']:
                if row[col] in sus_areas:
                    return True
            return False
        
        def time_to_HM(time_ms):
            time_spent = time_ms // 1000
            minutes, seconds = divmod(time_spent, 60)
            hours, minutes = divmod(minutes, 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        grouped_geofencing['Visited Sus Area'] = grouped_geofencing.apply(check_sus_area, axis=1) 
        grouped_geofencing = grouped_geofencing[grouped_geofencing['Visited Sus Area'] == True]

        cities = grouped_geofencing['city'].unique()
    
        df_threat = pd.DataFrame(columns = ['device_id','Suspicious Areas Visited','Time Spent'])

        new_rows = []

        for city in cities:

            sus_city_df = grouped_geofencing[grouped_geofencing['city'] == city]

            sus_city_df.loc[:, 'index_diff'] = pd.Series(np.diff(sus_city_df.index), index=sus_city_df.index[:-1])
            sus_city_df['jump'] = sus_city_df['index_diff'] > 1

            indices_of_jumps = np.where(np.array(list(sus_city_df['jump'])) == True)[0]
            indices_of_jumps = np.append(indices_of_jumps, len(sus_city_df))
            count_of_jumps = len(indices_of_jumps)

            index = 0
            sus_area_visited = random.choice(list(sus_city_df['country'])) + ' ' + random.choice(list(sus_city_df['provinance'])) + ' ' + city
            
            if count_of_jumps > 1:
                for visit in indices_of_jumps:
                    df_visit = sus_city_df[index:visit + 1]
                    time_spent = time_to_HM(max(df_visit['usage_timeframe']) - min(df_visit['usage_timeframe']))
                    time_info = [str(datetime.fromtimestamp(min(df_visit['usage_timeframe']) / 1000)), time_spent, str(datetime.fromtimestamp(max(df_visit['usage_timeframe']) / 1000))]
                    new_row = pd.Series({'device_id': df_device['device_id'][0], 'Suspicious Areas Visited': sus_area_visited, 'Time Spent': time_info})
                    index = visit + 1
                    new_rows.append(new_row)
            else:
                time_spent = time_to_HM(max(sus_city_df['usage_timeframe']) - min(sus_city_df['usage_timeframe']))
                time_info = [str(datetime.fromtimestamp(min(sus_city_df['usage_timeframe']) / 1000)), time_spent, str(datetime.fromtimestamp(max(sus_city_df['usage_timeframe']) / 1000))]
                sus_area_visited = random.choice(list(sus_city_df['country'])) + ' ' + random.choice(list(sus_city_df['provinance'])) + ' ' + city
                new_row = pd.Series({'device_id': df_device['device_id'][0], 'Suspicious Areas Visited': sus_area_visited, 'Time Spent': time_info})
                new_rows.append(new_row)

        df_threat = pd.DataFrame(new_rows, columns=df_threat.columns)

        return df_threat
    
    def get_sus_visits_list(self, df_history, sus_areas):
        grouped_dfs = dict(tuple(df_history.groupby('device_id')))
        grouped_result_dfs = {}
        for id in grouped_dfs.keys():
            group_df = grouped_dfs[id].reset_index(drop=True)
            grouped_result_dfs[id] = self.get_sus_visits(group_df, sus_areas)

        return grouped_result_dfs


    def get_contact_blacklist(self, df_device, df_blacklist_devices,tolerance_minutes:int=10,threshold_distance_m:int=25,distance:int=100):
        tolerance_ms = tolerance_minutes * 60000 # 10 minutes tolerance
        
        blacklist_devices = df_blacklist_devices['device_id'].unique()
        contacted_blacklist = []

        for blacklist_device in blacklist_devices:
            df_history = df_blacklist_devices[df_blacklist_devices['device_id'] == blacklist_device]
            df_device = self.utils.binning(df_device,distance=distance)
            df_device = self.utils.combine_coordinates(df_device)
            df_history = self.utils.binning(df_history,distance=distance)
            df_history = self.utils.combine_coordinates(df_history)

            df_merged = pd.merge_asof(df_device.sort_values('usage_timeframe'), df_history.sort_values('usage_timeframe'), 
                                on='usage_timeframe',by='grid',suffixes=('_MainDevice', '_BlacklistDevice'), 
                                direction='nearest', tolerance=tolerance_ms).dropna()
            if df_merged.empty:
                continue
            df_merged['distance'] = df_merged.apply(
                lambda row: geodesic(
                    (row['location_latitude_MainDevice'], row['location_longitude_MainDevice']),
                    (row['location_latitude_BlacklistDevice'], row['location_longitude_BlacklistDevice'])
                ).meters,
                axis=1
            )
            df_merged['crossed_path'] = df_merged['distance'] <= threshold_distance_m
            df_merged = df_merged.reset_index(drop=True)
            if df_merged['crossed_path'].any(): 
                contacted_blacklist.append(df_merged['device_id_BlacklistDevice'][0])
        if len(contacted_blacklist) != 0:
            result = contacted_blacklist
            return result
        else:
            result = None
            return result  
    
    def get_contact_blacklist_list(self, df_history, df_blacklist_devices):
        grouped_dfs = dict(tuple(df_history.groupby('device_id')))
        grouped_result_dfs = {}
        for id in grouped_dfs.keys():
            group_df = grouped_dfs[id].reset_index(drop=True)
            grouped_result_dfs[id] = self.get_contact_blacklist(group_df, df_blacklist_devices)

        return grouped_result_dfs

    
    def get_lifespan(self, df_device, max_time_gap):
        df_device = df_device.sort_values('usage_timeframe')
        df_device.loc[:, 'time_diff'] = pd.Series(np.diff(df_device['usage_timeframe']), index=df_device.index[:-1])
        gaps = df_device[df_device['time_diff'] > max_time_gap].index
        segments = [df_device.iloc[start:end] for start, end in zip([0] + list(gaps + 1), list(gaps) + [len(df_device)])]
        lifespan_info = []
        for segment in segments:
            first_timestamp = segment['usage_timeframe'].min()
            first_timestamp_date = self.utils.convert_ms_to_datetime_value(first_timestamp)
            last_timestamp = segment['usage_timeframe'].max()
            last_timestamp_date = self.utils.convert_ms_to_datetime_value(last_timestamp)
            lifespan = (last_timestamp_date - first_timestamp_date)
            lifespan_info.append({'first_seen': str(first_timestamp_date), 'last_seen': str(last_timestamp_date),'lifespan': str(lifespan)})
        return lifespan_info
    
    def get_lifespan_list(self, df_history, max_time_gap):
        grouped_dfs = dict(tuple(df_history.groupby('device_id')))
        grouped_result_dfs = {}
        for id in grouped_dfs.keys():
            group_df = grouped_dfs[id].reset_index(drop=True)
            grouped_result_dfs[id] = self.get_lifespan(group_df, max_time_gap)
        return grouped_result_dfs       


    def get_activity(self, df_device):
        first_activity = df_device['usage_timeframe'].min()
        first_activity = datetime.fromtimestamp(first_activity / 1000)
        last_activity = df_device['usage_timeframe'].max()
        last_activity = datetime.fromtimestamp(last_activity / 1000)
        return [str(first_activity), str(last_activity)]
    
    def get_activity_list(self, df_history):
        grouped_dfs = dict(tuple(df_history.groupby('device_id')))
        grouped_result_dfs = {}
        for id in grouped_dfs.keys():
            group_df = grouped_dfs[id].reset_index(drop=True)
            grouped_result_dfs[id] = self.get_activity(group_df)
        return grouped_result_dfs 


    def get_threat_analysis(self, df_device, sus_areas, df_blacklist_devices, max_time_gap,distance:int=100):        
        sus_area_df = self.get_sus_visits(df_device, sus_areas)
        sus_area_dict = dict(zip(sus_area_df['Suspicious Areas Visited'], sus_area_df['Time Spent']))
        contact_blacklist = self.get_contact_blacklist(df_device, df_blacklist_devices,distance=distance)

        lifespan_info = self.get_lifespan(df_device, max_time_gap)
        activity = self.get_activity(df_device)

        data = [[df_device['device_id'][0], sus_area_dict, contact_blacklist, activity, lifespan_info]]
        df_threat = pd.DataFrame(data, columns=['device_id', 'Sus Areas Visited & Time', 'Contaced Black Listed Device(s)', 'First & Last Activity','Lifespan Info'])
        return df_threat
    
    def get_threat_analysis_list(self, df_history, sus_areas, df_blacklist_devices, max_time_gap,distance:int=100):
        df_threat = pd.DataFrame(columns=['device_id', 'Sus Areas Visited & Time', 'Contaced Black Listed Device(s)', 'First & Last Activity', 'Lifespan Info'])
        grouped_dfs = dict(tuple(df_history.groupby('device_id')))
        for id in grouped_dfs.keys():
            group_df = grouped_dfs[id].reset_index(drop=True)
            grouped_result_df = self.get_threat_analysis(group_df, sus_areas, df_blacklist_devices, max_time_gap,distance=distance)
            df_threat = pd.concat([df_threat, grouped_result_df], ignore_index=True)
        return df_threat
    
