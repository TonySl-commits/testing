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

warnings.filterwarnings('ignore')

from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.plots.cotraveler_plots import CotravelerPlots
##################################################################################################################

class CotravelerFunctions():
    def __init__(self, verbose: bool = False):
        self.properties = CDR_Properties()
        self.utils = CDR_Utils()
        self.cassandra_tools = CassandraTools()
        self.cotraveler_plots = CotravelerPlots()
        self.verbose = verbose
        self.days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        self.day_order = {'Monday': 1, 'Tuesday': 2, 'Wednesday': 3, 'Thursday': 4, 'Friday': 5, 'Saturday': 6, 'Sunday': 7}
        self.cotraveler_template = """
            - Cotraveler {cotraveler_id}:
            Id: {cotraveler_id}
            Start date: {cotraveler_start_date}
            End date: {cotraveler_end_date}
            Number of hits: {cotraveler_id_hits}
            
        """

        self.user_template = """
            -Main ID:
            Id: {main_id}
            Start date: {main_start_date}
            End date:  {main_end_date}
            Number of hits: {main_id_hits}

            {cotraveler_info}
        """
        pd.set_option('display.max_columns', None)

    def lcs(self,sequence1, sequence2):

        if len(sequence1) != len(sequence2):
            raise ValueError("Sequences have different lengths")
        #array of empty tuples
        lcs_array = [0] 
        m = len(sequence1)

        sequence1 = [tuple(seq) for seq in sequence1]
        sequence2 = [tuple(seq) for seq in sequence2]
        for i, j in zip(range(1, m+1), range(1, m+1)):
            if sequence1[i-1] == sequence2[j-1]:
                lcs_array.append((lcs_array[i-1]+1))
            else:

                lcs_array.append(0)

        return max(lcs_array)  

    def lcs2(self,sequence1, sequence2,distance:int=100):
        if len(sequence1)==0:
            return -1
        i=0
        lcs_array = [0] 
        step_lat, step_lon = self.utils.get_step_size(distance=distance)
        for (seq1, seq2) in zip(sequence1, sequence2):
            if  (seq1[0]-step_lat <= seq2[0] <= seq1[0]+step_lat) and (seq1[1]-step_lon <= seq2[1] <= seq1[1]+step_lon):
                lcs_array.append((lcs_array[i]+1))
                i+=1
            else:
                lcs_array.append(0)
                i+=1
        return max(lcs_array)

    def common_places(self, data):
        data = data[['device_id','grid']]
        grouped = data.groupby(['device_id']).nunique('grid').reset_index()
        grouped = grouped.sort_values(by='grid', ascending=False)
        return grouped

    def add_existing_binning(self,df_device,df_history,grid_column:str='grid'):
        device_id = df_device['device_id'].unique()[0]
        df_device = df_history[df_history['device_id'] == device_id]
        df_device.reset_index(drop=True, inplace=True)
        df_device = df_device.drop_duplicates(subset=['device_id','location_latitude','location_longitude','usage_timeframe'])
        return df_device
    def combine_coordinates(self, data, longitude_column : str = 'longitude_grid', latitude_column : str = 'latitude_grid'):
        data.loc[:, 'grid'] = data[latitude_column].astype(str) + ','\
            + data[longitude_column].astype(str)
        return data

    def str_date_to_millisecond(self, date, format='%Y-%m-%d'):
        timestamp = int(datetime.strptime(date,'%Y-%m-%d').timestamp() * 1000)
        return timestamp

    def add_visit_columns(self,data,time_diff:int=5,timestamp_column:str='usage_timeframe',grid_column:str='grid'):
        time_diff = time_diff * 3600 # in hours

        visits = []
        prev_grid = None
        start_time = None
        end_time = None
        data['next_timestamp'] = data[timestamp_column].shift(-1)

        for _, data in data.iterrows():
            timestamp = data[timestamp_column]
            grid = data[grid_column]

            if prev_grid is None:
                # First data point
                prev_grid = grid
                start_time = timestamp
                end_time = timestamp
            elif grid == prev_grid and (timestamp - pd.Timestamp(end_time) 
                                        ).total_seconds() <= time_diff and timestamp.date(
                                        ) == pd.Timestamp(end_time).date():
                end_time = timestamp
            else:
                # Start a new visit
                if prev_grid is not None:
                    duration = end_time - start_time
                    visit = {
                        grid_column: prev_grid,
                        'start_time': start_time,
                        'end_time': end_time,
                        'duration': duration
                    }
                    visits.append(visit)

                prev_grid = grid
                start_time = timestamp
                end_time = None

                if pd.notnull(data['next_timestamp']):
                    end_time = data['next_timestamp']

        df_visits = pd.DataFrame(visits)
        return df_visits

    def filter_visit_columns(self,data,min_threshold:int=1,add_mintutes:int=1,grid_column:str='grid'):
        data = self.utils.convert_ms_to_datetime(data,'start_time')
        data = self.utils.convert_ms_to_datetime(data,'end_time')

        data['duration_minutes'] = (data['end_time'] - data['start_time']).dt.total_seconds() / 60

        mask = data['duration_minutes'] < min_threshold
        data.loc[mask, 'start_time'] -= pd.Timedelta(minutes=add_mintutes)
        data.loc[mask, 'end_time'] += pd.Timedelta(minutes=add_mintutes)

        grouped = data.groupby(grid_column)\
            .agg({"start_time":list, "end_time":list})
            
        grouped['time_intervals_start'] = grouped\
            .apply(lambda x: [(start, end) for start, end in zip(x['start_time'], x['end_time'])], axis=1)
            
        grouped_df_max_min = data.groupby(grid_column)\
            .agg({'start_time': 'min', 'end_time': 'max'}).reset_index()

        grouped = grouped.drop(['start_time', 'end_time'], axis=1)
        grouped['start_time'] = list(grouped_df_max_min['start_time'])
        grouped['end_time'] = list(grouped_df_max_min['end_time'])
        data = grouped.reset_index()

        data[['latitude_grid','longitude_grid']] = data[grid_column].str.split(',',expand=True).astype(float)

        data = self.utils.convert_datetime_to_ms(data,'start_time')
        data = self.utils.convert_datetime_to_ms(data,'end_time')
        
        data = data.reset_index(drop=True)
        data = data.drop(grid_column, axis=1)
        return data

    def device_scan_query_builder(self,  start_date, end_date , scan_distance, latitude, longitude,region,sub_region):
        query = self.properties.device_scan_query
        date = datetime.fromtimestamp(start_date / 1000)
        year = str(date.year)
        month = str(date.month)
        lat = str(latitude)
        longitude = str(longitude)
        start_date = str(start_date)
        end_date = str(end_date)
        scan_distance = str(scan_distance)
        region = str(region)
        sub_region = str(sub_region)

        query = query.replace('year', year)
        query = query.replace('month', month)
        query = query.replace('value1', lat)
        query = query.replace('value2', longitude)
        query = query.replace('value3', start_date)
        query = query.replace('value4', end_date)
        query = query.replace('scan_distance', scan_distance)
        query = query.replace('region',region)
        query = query.replace('subre',sub_region)
        return query

    

    def get_common_df(self, data,scan_distance:int=200 ,region:int = 142 , sub_region:int =145 ,passed_server:str='10.10.10.101',default_timeout:int=10000,default_fetch_size:int=10000,session=None):

        scan_distance = ((scan_distance*math.sqrt(2))/2)
        x =0
        data_retrived = 0 
        df_list = []
 
        for index, row in data.iterrows():
            df = pd.DataFrame()
            
            query = self.device_scan_query_builder(start_date = row['start_time'],
                                                    end_date = row['end_time'],
                                                    scan_distance = scan_distance,
                                                    latitude = row['latitude_grid'],
                                                    longitude = row['longitude_grid'],
                                                    region = region,
                                                    sub_region=sub_region)
   
            try:
                df1 = session.execute(query)
                df = pd.DataFrame(df1.current_rows)

            except Exception as e:
                print("exception:",index)
                print(e)
                continue

            if len(df) != 0:
                df = df[['device_id',
                        'location_latitude',
                        'location_longitude',
                        'usage_timeframe',
                        'location_name',
                        'service_provider_id']].drop_duplicates()

                df.loc[:,'grid'] =  str(row['latitude_grid']) + ',' + str(row['longitude_grid'])
                df = self.utils.convert_ms_to_datetime(df)
                intervals_list=row['time_intervals_start']
                if len(row['time_intervals_start'])!=1:
                    mask = df.apply(lambda row2: any( start <= row2['usage_timeframe']<= end \
                        for start, end in intervals_list), axis=1)
                    df = df[mask]
                data_retrived+=len(df)
                df_list.append(df)

            if len(df) == 0 :
                x += 1
            print("★★★★★★★★★★ Requests remaining: {:<4} ★★★★★★★★★★ Empty Requests: {:<4} ★★★★★★★★★★ Data Retrieved: {:<6} ★★★★★★★★★★"\
                .format(len(data)-index, x, data_retrived))

        print('empty:',x)
        common_df = pd.concat(df_list)
        print('COMMON DF BEFORE: ',common_df.shape)
        common_df = common_df.drop_duplicates(subset=['device_id','location_latitude','location_longitude','usage_timeframe'])
        print('COMMON DF AFTER: ',common_df.shape)
        
        return common_df

    def get_device_list(self,data,grid_column:str='grid'):
        data = data[['device_id',grid_column]]
        grouped = data.groupby(['device_id']).nunique(grid_column).reset_index()
        grouped = grouped.sort_values(by=grid_column, ascending=False)
        device_list = grouped['device_id'].unique().tolist()
        return device_list

    def check_device_id_print(self,device_id_to_check:str,device_list:list):
        if device_id_to_check in device_list:
            print("\nCorrelation exists for device_id:", device_id_to_check,'\n')
        else:
            print("\nCorrelation does not exist for device_id:", device_id_to_check,'\n')
            
    def separate_list(self, lst, num_lists):
        sublist_size = len(lst) // num_lists
        remainder = len(lst) % num_lists
        sublists = []
        index = 0
        for i in range(num_lists):
            sublist = lst[index:index + sublist_size]
            if remainder > 0:
                sublist.append(lst[-remainder])
                remainder -= 1
            sublists.append(sublist)
            index += sublist_size
        return sublists

    def preprocess_for_sequence(self,df):
        # if df['usage_timeframe'].dtype == int:
        #     df = self.utils.convert_ms_to_datetime(df)
        
        # df.index = df['usage_timeframe']
        return df
        
    def common_sequence(self, device_list, df_device,df_history ,distance_method:float=0.5,distance:int=100,minutes:int=5):
        result = []
        df_merged_list = []
        t1 = time.time()
        count = 0
        df_device['location_name'] = df_device['location_name'].fillna(df_device['device_id'].head(1).values[0])
        for device_id in device_list:
            df_cotraveler = df_history[df_history['device_id'] == device_id]
            df_main = df_device.copy()    
            df_main = df_main.reset_index(drop=True).sort_values('usage_timeframe')
            df_cotraveler = df_cotraveler.reset_index(drop=True).sort_values('usage_timeframe')
            df_cotraveler['location_name'] = df_cotraveler['location_name'].fillna(df_cotraveler['device_id'].head(1).values[0])

            tol = minutes * 60 * 1000
            df_main['usage_timeframe'] = df_main['usage_timeframe'].astype(np.int64)
            df_cotraveler['usage_timeframe'] = df_cotraveler['usage_timeframe'].astype(np.int64)
            try:
                df_main.drop('Unnamed: 0',axis=1,inplace=True)
                # print("trydrop",df_main.shape)
            except:
                pass
            
            try:
                df_cotraveler.drop('Unnamed: 0',axis=1,inplace=True)
            except:
                pass

            df_merged = pd.merge_asof(left=df_main,
                                    right=df_cotraveler,
                                    on='usage_timeframe',
                                    direction='nearest',
                                    tolerance=tol).dropna()

            df_merged_list.append(df_merged)

            if df_merged.empty:
                continue
            # print("before merge",df_main.shape)
            df_main = df_merged[['device_id_x',
                                    'location_latitude_x',
                                    'location_longitude_x',
                                    'usage_timeframe',
                                    'latitude_grid_x',
                                    'longitude_grid_x',
                                    'grid_x']]
            df_cotraveler = df_merged[['device_id_y',
                                'location_latitude_y',
                                'location_longitude_y',
                                'usage_timeframe',
                                'latitude_grid_y',
                                'longitude_grid_y',
                                'grid_y']]

            df_main.columns = df_main.columns.str.replace('_x', '')
            df_cotraveler.columns = df_cotraveler.columns.str.replace('_y', '')

            df_cotraveler = df_cotraveler.reset_index(drop=True)
            df_main = df_main.reset_index(drop=True)

            df_cotraveler =df_cotraveler.sort_values('usage_timeframe')
            df_main = df_main.sort_values('usage_timeframe')

            devices_sequence_list = df_cotraveler['grid'].str.split(',').apply(lambda x: [float(i) for i in x]).tolist()
            main_sequence_list = df_main['grid'].str.split(',').apply(lambda x: [float(i) for i in x]).tolist()

            if count == 0:
                print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('LCS/DTW Started'.center(50)))
            common_num = self.lcs2(devices_sequence_list, main_sequence_list,distance = distance)

            dist, path = fastdtw.fastdtw(devices_sequence_list, main_sequence_list, dist=distance_method)
            if count == len(device_list)-1:
                print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('LCS/DTW Ended'.center(50)))
            result.append((device_id,dist,common_num))
            count += 1

        df_merged_devices = pd.concat(df_merged_list) 
        
        print(f"LCS Time: {time.time() -t1}")

        return result , df_merged_devices 

    def add_visit_count_column(self,table,df_common):
        device_counts = df_common['device_id'].value_counts()
        device_counts_df = device_counts.reset_index()
        device_counts_df.columns = ['device_id', 'count']
        table = pd.merge(table, device_counts_df, on='device_id', how='left')
        table=table.sort_values(by='count', ascending=False)
        table = table[table['count'] > -1]
        return table

    def add_statistics_columns(self,table,df_device,df_history,distance:int=100):
        unique_device_list = list(table['device_id'].unique())
        common_results ,df_merged_devices = self.common_sequence(device_list=unique_device_list ,
                                              df_device=df_device,
                                              df_history=df_history,
                                              distance=distance,
                                              distance_method=0.5) # type: ignore

        common_results = pd.DataFrame(common_results, columns=['device_id', 'sequence_distance','longest_sequence'])
        table = pd.merge(table,common_results,on='device_id')
        table=table.sort_values(by=['count','longest_sequence','sequence_distance'], ascending=False)
        table.reset_index(drop=True, inplace=True)
        table.insert(0, 'Rank', range(1, len(table) + 1))
        return table , df_merged_devices
    
    def add_common_locations_hits_coulmn(self,table,df_common):
        df_common = self.utils.convert_datetime_to_ms(df_common,self.properties.usage_timeframe)
        df_common = df_common.groupby('device_id').apply(self.create_location_array).reset_index(name='COMMON_LOCATIONS_HITS')
        table = pd.merge(table,df_common,on='device_id')
        return table

    def create_location_array(self,group):
        return [
            list(x) for x in zip(group[self.properties.location_latitude],
            group[self.properties.location_longitude],
            group['device_id'],
            group[self.properties.usage_timeframe],
            group['location_name'],
            group[self.properties.service_provider_id])
        ]


    def add_percentage_column(self,table,df_merged_devices,distance,total_merged_df):
        step_lat, step_lon = self.utils.get_step_size(distance=distance)

        custom_filter = lambda row: (row['latitude_grid_x']-step_lat <= row['latitude_grid_y'] <= row['latitude_grid_x']+step_lat) and (row['longitude_grid_x']-step_lon <= row['longitude_grid_y'] <= row['longitude_grid_x']+step_lon)
        df_common = df_merged_devices[df_merged_devices.apply(custom_filter, axis=1)]
        df_common =df_common[['device_id_y','location_latitude_y','location_longitude_y','usage_timeframe','latitude_grid_y','longitude_grid_y','grid_y','provinance_y','country_y','city_y','lat_long_y']]
        df_common.columns = ['device_id','location_latitude','location_longitude','usage_timeframe','latitude_grid','longitude_grid','grid','provinance','country','city','lat_long']
        device_counts_df = df_common['device_id'].value_counts().reset_index()
        device_counts_df.columns = ['device_id', 'COMMON_LOCATIONS_HITS']


        device_provinance_df = df_common.groupby('device_id')['provinance'].agg(lambda x: list(set(x))).reset_index()
        device_country_df = df_common.groupby('device_id')['country'].agg(lambda x: list(set(x))).reset_index()
        device_cities_df = df_common.groupby('device_id')['city'].agg(lambda x: list(set(x))).reset_index()
        device_lat_long_df = df_common.groupby('device_id')['lat_long'].agg(lambda x: list(set(x))).reset_index()
        # device_tuples_df = df_common.groupby('device_id')['country','city','lat_long'].agg(lambda x: list(x)).reset_index()
        df_common['usage_timeframe_date'] = pd.to_datetime(df_common['usage_timeframe'], unit='ms').dt.strftime('%Y/%m/%d %H:00:00')
        df_grouped = df_common.groupby(['device_id','country','city','lat_long','usage_timeframe_date']).size().reset_index(name='Number_Of_Hits_per_country')
        country_tuples = df_grouped.groupby('device_id').apply(lambda x: x[['country', 'city', 'lat_long', 'Number_Of_Hits_per_country','usage_timeframe_date']].values.tolist()).reset_index(name='COUNTRY_TUPLES')
        table['GRID_SIMILARITY'] = device_counts_df['COMMON_LOCATIONS_HITS'].div(total_merged_df['TOTAL_MERGED_HITS'])
        table['GRID_SIMILARITY'] = table['GRID_SIMILARITY'].fillna(0)
        table['COMMON_PROVINANCES'] = device_provinance_df['provinance']
        table['COMMON_COUNTRIES'] = device_country_df['country']
        table['COMMON_CITIES'] = device_cities_df['city']
        table['COMMON_LAT_LONG'] = device_lat_long_df['lat_long']
        table = pd.merge(table, country_tuples, on='device_id', how='inner')
        table = table[table['GRID_SIMILARITY']>=0.05]
        table.reset_index(inplace=True,drop=True)
        return table
    
    def get_uncommon_df(self,df_merged_devices,distance):
        step_lat, step_lon = self.utils.get_step_size(distance=distance)
        custom_filter = lambda row: (row['latitude_grid_x']-step_lat <= row['latitude_grid_y'] <= row['latitude_grid_x']+step_lat) and (row['longitude_grid_x']-step_lon <= row['longitude_grid_y'] <= row['longitude_grid_x']+step_lon)
        df_uncommon = df_merged_devices[~df_merged_devices.apply(custom_filter, axis=1)]

        df_uncommon =df_uncommon[['device_id_y','location_latitude_y','location_longitude_y','usage_timeframe','latitude_grid_y','longitude_grid_y','grid_y']]
        df_uncommon.columns = ['device_id','location_latitude','location_longitude','usage_timeframe','latitude_grid','longitude_grid','grid']
        df_uncommon = df_uncommon.drop_duplicates(subset=['device_id','location_latitude','location_longitude','usage_timeframe'])
        return df_uncommon
    
    def get_total_merged_df(self,df_merged_devices):
        total_merged_df = df_merged_devices['device_id_y'].value_counts().reset_index()
        total_merged_df.columns = ['device_id', 'TOTAL_MERGED_HITS']
        return total_merged_df

    def add_uncommon_locations_hits_coulmn(self, table,df_uncommon):
        device_counts_df = df_uncommon['device_id'].value_counts().reset_index()
        device_counts_df.columns = ['device_id', 'UNCOMMON_LOCATIONS_HITS']
        table = pd.merge(table, device_counts_df, on='device_id', how='left')
        table['UNCOMMON_LOCATIONS_HITS'] = table['UNCOMMON_LOCATIONS_HITS'].fillna(0).astype(int)
        return table
    
    def add_hits_count_column(self,df_history):
        table = pd.DataFrame()
        table['device_id'] =  df_history['device_id'].unique()
        df_count = df_history['device_id'].value_counts().reset_index()
        df_count.columns = ['device_id', 'TOTAL_HITS']
        table = pd.merge(table, df_count, on='device_id', how='left')
        return table
    
    def add_uncommon_ratio_column(self, table, total_merged_df):
        table = pd.merge(table, total_merged_df, on='device_id', how='left')
        table['UNCOMMON_HITS_RATIO'] = table['UNCOMMON_LOCATIONS_HITS']/table['TOTAL_MERGED_HITS']
        return table
    
    def add_uncommon_timespent_column(self,table,df_merged_devices,distance):
        result = []
        step_lat, step_lon = self.utils.get_step_size(distance=distance)
        def uncommon_time_list(group):
            changes = []
            start = None
            prev_row = None
            end = None
            for _, row in group.iterrows():
                x=0
                if prev_row is None:
                    prev_row = row
                if start is None: #ONE TIME
                    if not ((row['latitude_grid_x']-step_lat < row['latitude_grid_y'] < row['latitude_grid_x']+step_lat) and (row['longitude_grid_x']-step_lon < row['longitude_grid_y'] < row['longitude_grid_x']+step_lon)):
                        start = row['usage_timeframe']
                        x=x+1
                elif start is not None and ((row['latitude_grid_x']-step_lat <= row['latitude_grid_y'] <= row['latitude_grid_x']+step_lat) and (row['longitude_grid_x']-step_lon <= row['longitude_grid_y'] <= row['longitude_grid_x']+step_lon)):
                    end = prev_row['usage_timeframe']
                    changes.append([start, end])
                    start = None
                    end=None
                prev_row = row
            if start is not None and end is None:
                changes.append([start, group['usage_timeframe'].iloc[-1]])
            return changes

        result = df_merged_devices.groupby('device_id_y').apply(uncommon_time_list).reset_index()
        result.columns = ['device_id','UNCOMMON_LOCATIONS_RANGE']
        table = pd.merge(table,result,on='device_id')
        table['TOTAL_UNCOMMMON_TIME_SPENT'] = table['UNCOMMON_LOCATIONS_RANGE'].apply(lambda x: sum([range_[1] - range_[0] for range_ in x]))
        table['TOTAL_UNCOMMMON_TIME_SPENT'] = table['TOTAL_UNCOMMMON_TIME_SPENT']/1000
        table['AVERAGE_UNCOMMMON_TIME_SPENT'] = table['TOTAL_UNCOMMMON_TIME_SPENT'] / table['UNCOMMON_LOCATIONS_RANGE'].apply(len)
        table['AVERAGE_UNCOMMMON_TIME_SPENT'] = table['AVERAGE_UNCOMMMON_TIME_SPENT'].fillna(0)
        table['TOTAL_UNCOMMMON_TIME_SPENT'] = table['TOTAL_UNCOMMMON_TIME_SPENT'].apply(self.utils.seconds_to_hhmmss)
        table['AVERAGE_UNCOMMMON_TIME_SPENT'] = table['AVERAGE_UNCOMMMON_TIME_SPENT'].apply(self.utils.seconds_to_hhmmss)


        return table

    # def add_common_cities_coulmn(self,table,df_merged_devices):
        
    def add_classification_column(self,table):
        longest_sequence_threshold = 5 # Example threshold for a high longest sequence
        sequence_distance_threshold = 0.02 # Example threshold for a low sequence distance
        grid_similarity_threshold = 0.4 # Example threshold for a high grid similarity
        
        # Function to classify based on the thresholds
        def classify(row):
            if row['LONGEST_SEQUENCE'] > longest_sequence_threshold and row['SEQUENCE_DISTANCE'] < sequence_distance_threshold and row['GRID_SIMILARITY'] > grid_similarity_threshold:
                if row['LONGEST_SEQUENCE'] > 30:
                    return 'shadow'
                else:
                    return 'companion'
            else:
                return 'other'
        
        # Apply the classification function to each row
        table['CLASSIFICATION'] = table.apply(classify, axis=1)

        print(table[['DEVICE_ID','LONGEST_SEQUENCE','GRID_SIMILARITY','SEQUENCE_DISTANCE','CLASSIFICATION']])
        return table
    
    def get_barcharts_all_devices(self,date_counts_main,date_counts_cotraveler):
        date_counts_cotraveler = pd.concat([date_counts_main, date_counts_cotraveler])
        
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        date_counts_cotraveler['all_dates'] = pd.Categorical(date_counts_cotraveler['all_dates'], categories=days_order, ordered=True)

        fig, ax = plt.subplots(figsize=(10, 6))
        
        palette = ["#fee090","#fdae61","#4575b4","#313695","#e0f3f8","#abd9e9","#d73027", "#a50026"]
        
        main_device_id = date_counts_main['device_id'].iloc[0]
        main_device_data = date_counts_cotraveler[date_counts_cotraveler['device_id'] == main_device_id]
        sns.barplot(x='all_dates', y='count', data=main_device_data, ax=ax, color=palette[0], label=main_device_id, ci=None)
        
        other_devices_data = date_counts_cotraveler[date_counts_cotraveler['device_id'] != main_device_id]
        num_other_devices = len(other_devices_data['device_id'].unique())
        for idx, (device_id, group) in enumerate(other_devices_data.groupby('device_id')):
            device_color = palette[idx + 1] 
            sns.barplot(x='all_dates', y='count', data=group, ax=ax, color=device_color, label=device_id, ci=None)

            for bar in ax.patches[-len(group):]:
                bar.set_width(bar.get_width() / num_other_devices+0.1)
                bar.set_x(bar.get_x() + idx * bar.get_width() / num_other_devices)
        
        ax.set_title('Distribution of Hits per Day of the Week')
        ax.set_xlabel('Day of the Week')
        ax.set_ylabel('Count')
        
        handles, labels = ax.get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        ax.legend(by_label.values(), by_label.keys(), title='Device ID')
        return plt
    
    def convert_start_end_to_dow(self,row):
        dates = pd.date_range(start=row['start_time'], end=row['end_time'], freq='D', inclusive='both')
        days_of_week = dates.to_series().dt.day_name().tolist()
        end_day_of_week = pd.to_datetime(row['end_time']).strftime('%A')
        
        if days_of_week[-1] != end_day_of_week:
            days_of_week.append(end_day_of_week)
        return days_of_week

    def add_days_list(self,df_visit):
        df_visit['all_dates'] = df_visit.apply(self.convert_start_end_to_dow, axis=1)
        return df_visit

    def get_main_device_stats(self,df_main):
        main_id = df_main.dropna(subset=['device_id'])['device_id'].iloc[0]
        main_id_hits = df_main.shape[0]
        df_main['days'] = pd.to_datetime(df_main['usage_timeframe'],unit='ms').dt.day_name()
        unique_days_main = df_main.groupby(['device_id', 'days']).size().to_dict()

        for day in self.days_of_week:
            unique_days_main.setdefault((main_id, day), 0)
            unique_days_main = dict(sorted(unique_days_main.items(), key=lambda item: self.day_order[item[0][1]]))

        df_main['date'] = pd.to_datetime(df_main['usage_timeframe'],unit='ms').dt.date
        main_start_end_list = df_main.groupby('device_id')['date'].agg(['min', 'max']).reset_index()

        return main_id,main_id_hits,unique_days_main,main_start_end_list

    def get_cotravelers_stats(self,df_history):
        cotraveler_list = df_history.dropna(subset=['device_id'])['device_id'].unique().tolist()
        cotravelers_hits = df_history.groupby('device_id').size().reset_index(name='hits')

        df_history['date'] = pd.to_datetime(df_history['usage_timeframe'],unit='ms').dt.date
        cotraveler_start_end_list = df_history.groupby('device_id')['date'].agg(['min', 'max']).reset_index()
        return cotraveler_list,cotravelers_hits,cotraveler_start_end_list


    def get_distribution_difference(self,df_main_visit,df_cotraveler_visit):
        all_dates_flat = [date for sublist in df_main_visit['all_dates'] for date in sublist]
        date_counts_main = pd.Series(all_dates_flat).value_counts().reset_index(name='count')
        date_counts_main['device_id'] = df_main_visit['device_id'].iloc[0] 
        date_counts_main = date_counts_main.rename(columns={'index':'all_dates'})
        date_counts_main = date_counts_main[['device_id' , 'all_dates'  ,'count']]
        df_exploded = df_cotraveler_visit.explode('all_dates')
        day_counts_cotraveler = df_exploded.groupby(['device_id', 'all_dates']).size().reset_index(name='count')
        return date_counts_main,day_counts_cotraveler


    def generate_user_input_and_barchart(self,df_main,df_history,df_common):
        main_id,\
        main_id_hits,\
        unique_days_main,\
        main_start_end_list = self.get_main_device_stats(df_main)

        cotravelers_ids,\
        cotravelers_hits,\
        cotraveler_start_end_list = self.get_cotravelers_stats(df_history)

        step_lat,step_lon = self.utils.get_step_size(50)
        df_visits_main= self.utils.get_visit_df(df=df_main,step_lat=step_lat,step_lon = step_lon)
        df_common['usage_timeframe'] = pd.to_datetime(df_common['usage_timeframe'])
        df_common = df_common.sort_values(['device_id','usage_timeframe'])
        df_visits_cotraveler = pd.concat([self.utils.get_visit_df(df=group.reset_index(drop=True),step_lat=step_lat,step_lon=step_lon) for _, group in df_common.groupby('device_id')])
        

        df_visits_main = self.add_days_list(df_visits_main)
        df_visits_cotraveler =  self.add_days_list(df_visits_cotraveler)


        date_counts_main,date_counts_cotraveler = self.get_distribution_difference(df_visits_main,df_visits_cotraveler)
        
        date_counts_main.to_csv('test1.csv')
        date_counts_cotraveler.to_csv('test2.csv')
        
        distribution_barchart = self.cotraveler_plots.get_barchart(date_counts_main,date_counts_cotraveler)
        cotraveler_list = []

        for i in range(len(cotravelers_ids)):
            cotraveler_id = cotravelers_ids[i]
            cotraveler_start_date, cotraveler_end_date = cotraveler_start_end_list[cotraveler_start_end_list['device_id'] == cotraveler_id]['min'].iloc[0],cotraveler_start_end_list[cotraveler_start_end_list['device_id'] == cotraveler_id]['max'].iloc[0]
            cotraveler_id_hits = cotravelers_hits[cotravelers_hits['device_id']==cotraveler_id]['hits'].iloc[0]
            date_counts_cotraveler_one = date_counts_cotraveler[date_counts_cotraveler['device_id'] == cotraveler_id] 
            date_counts_cotraveler_one = date_counts_cotraveler_one[['all_dates','count']]

            cotraveler_string = self.cotraveler_template.format(
                cotraveler_id=cotraveler_id,
                cotraveler_start_date=cotraveler_start_date,
                cotraveler_end_date=cotraveler_end_date,
                cotraveler_id_hits=cotraveler_id_hits,
            )

            cotraveler_list.append(cotraveler_string)
            
            user_input = self.user_template.format(
                main_id=main_id,
                main_start_date=main_start_end_list['min'].iloc[0],
                main_end_date=main_start_end_list['max'].iloc[0],
                main_id_hits=main_id_hits,
                cotraveler_info=" ".join(cotraveler_list)
            )

        # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        # print(user_input)
        # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

        return user_input, distribution_barchart
    
    def add_common_timespent_column(self,table,df_common_visit):
        print(df_common_visit,df_common_visit.columns)
        #calculate duration knowing that i have start_date and end_date columns
        df_common_visit['duration'] = df_common_visit['end_date'] - df_common_visit['start_date']

        #give me total time spent 
        table['COMMON_TIMESPENT'] = df_common_visit.groupby('device_id')['duration'].sum()
        table['COMMON_TIMESPENT'] = table['COMMON_TIMESPENT'].fillna(0)
        print(table,table.columns)
        return table
    