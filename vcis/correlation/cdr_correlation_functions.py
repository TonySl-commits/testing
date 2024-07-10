import pandas as pd
from datetime import datetime
from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools

import numpy as np
import fastdtw
import warnings
import math
import math

import time
warnings.filterwarnings('ignore')
##################################################################################################################

class CDRCorrelationFunctions():
    def __init__(self, verbose: bool = False):
        self.properties = CDR_Properties()
        self.utils = CDR_Utils()
        self.cassandra_tools = CassandraTools()
        self.verbose = verbose  
    
    def common_places(self, data):
        data = data[['device_id','grid']]
        grouped = data.groupby(['device_id']).nunique('grid').reset_index()
        grouped = data.groupby(['device_id']).nunique('grid').reset_index()
        grouped = grouped.sort_values(by='grid', ascending=False)
        return grouped
    
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
        print(data['start_time'])
        
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
        print(data)
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
        
    def device_scan_month_year_query_builder(self, start_date, end_date, year, month, scan_distance, latitude, longitude,region,subre):
        query = self.properties.device_scan_query

        year = str(year)
        month = str(month)

        lat = str(latitude)
        longitude = str(longitude)

        start_date = str(start_date)
        end_date = str(end_date)

        scan_distance = str(scan_distance)
        region = str(region)
        subre = str(subre)
        query = query.replace('year', year) \
            .replace('month', month) \
            .replace('value1', lat) \
            .replace('value2', longitude) \
            .replace('value3', start_date) \
            .replace('value4', end_date) \
            .replace('scan_distance', scan_distance) \
            .replace('region', region) \
            .replace('subre', subre)

        return query
    
    def get_device_list_from_device_scan(self, start_date, end_date, latitude, longitude, scan_distance : int = 200, passed_server : str = '10.1.10.110', default_timeout : int = 10000, default_fetch_size : int = 10000,region:int=142,subre:int=145,session=None):
        session = self.cassandra_tools.get_cassandra_connection(passed_server,
                                                  default_timeout=default_timeout,
                                                  default_fetch_size=default_fetch_size)
        
        device_list = []
        df_list = []
        
        year_month_combinations  = self.utils.get_month_year_combinations(start_date, end_date)
        # data = self.utils.convert_datetime_to_ms(data,'start_time')
        # data = self.utils.convert_datetime_to_ms(data,'end_time')

        for year, month in year_month_combinations:
            df = pd.DataFrame()
            query = self.device_scan_month_year_query_builder(start_date = start_date,
                                                end_date = end_date,
                                                year = year,
                                                month = month,
                                                scan_distance = scan_distance,
                                                latitude = latitude,
                                                longitude = longitude,
                                                region=region,
                                                subre=subre)

            try:
                df = session.execute(query)
                df = pd.DataFrame(df.current_rows)

            except Exception as e:
                print("Exception raised:")
                print(e)

            if len(df) != 0:
                df = df[['location_latitude', 'location_longitude', 'device_id', 'usage_timeframe','location_name','service_provider_id']].drop_duplicates()
                df_list.append(df)

        common_df = pd.concat(df_list)
        device_list = common_df['device_id'].unique().tolist()
        
        return common_df, device_list

    
    def get_common_df(self, data,scan_distance:int=200 ,region:int = 142 , sub_region:int =145 ,passed_server:str='10.10.10.101',default_timeout:int=10000,default_fetch_size:int=10000):

        session = self.cassandra_tools.get_cassandra_connection(passed_server,
                                                                default_timeout=default_timeout,
                                                                default_fetch_size=default_fetch_size)
        scan_distance = ((scan_distance*math.sqrt(2))/2)
        x =0
        data_retrived = 0 
        df_list = []
        acb  =1   
        for index, row in data.iterrows():
            df = pd.DataFrame()
            
            query = self.device_scan_query_builder(start_date = row['start_time'],
                                                    end_date = row['end_time'],
                                                    scan_distance = scan_distance,
                                                    latitude = row['latitude_grid'],
                                                    longitude = row['longitude_grid'],
                                                    region = region,
                                                    sub_region=sub_region)
            if acb==1:
                acb==0
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
            print("/nCorrelation exists for device_id:", device_id_to_check,'/n')
        else:
            print("/nCorrelation does not exist for device_id:", device_id_to_check,'/n')
            
            
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
    
    def add_percentage_column(self,df_history,df_common):
        df_history_grouped = df_history.groupby('device_id').count().reset_index()\
            [['device_id','usage_timeframe']]
        df_common_grouped = df_common.groupby('device_id').count().reset_index()\
            [['device_id','usage_timeframe']]
    
        df_merged = pd.merge(df_common_grouped, df_history_grouped,on='device_id')
        df_merged.rename(columns={'usage_timeframe_x': 'common_record' , 'usage_timeframe_y': 'history_record'}, inplace=True) 
        df_merged['percentage'] =(df_merged['common_record']/df_merged['history_record'])*100
        df_merged_hits = df_merged[['device_id','percentage']]
        
        history_grouped = df_history.groupby('device_id')['grid'].nunique().reset_index()
        common_grouped =self.common_places(df_common)

        merged_grid = pd.merge(history_grouped,common_grouped,on='device_id')

        merged_grid = merged_grid.rename(columns={'grid_x' : 'history_record' , 'grid_y':'common_record'})
        merged_grid['percentage'] = (merged_grid['common_record']/merged_grid['history_record'])*100
        df_merged_grids = merged_grid[['device_id','percentage']]
  
        table = pd.merge(df_merged_grids,df_merged_hits,on = 'device_id')
        table = table.rename(columns={'percentage_x' : 'grid_similarity','percentage_y' : 'hits_similarity'})
        return table
    
    def lcs(self,sequence1, sequence2):
        if len(sequence1)==0:
            return -1
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
      
    def lcs2(self, sequence1, sequence2,distance:int=100):
        print(len(sequence1))
        if len(sequence1)==0:
            return -1        
        i=0
        lcs_array = [0] 
        step_lat, step_lon = self.utils.get_step_size(distance=distance)
        for (seq1, seq2) in zip(sequence1, sequence2):
            if  (seq1[0]-step_lat <= seq2[0] <= seq1[0]+step_lat) and (seq1[1]-step_lon <= seq2[1] <= seq1[1]+step_lon):
                lcs_array.append((lcs_array[i-1]+1))
            else:
                lcs_array.append(0)
            i+=1
        return max(lcs_array)
    
    def preprocess_for_sequence(self,df,rename=False):
        try:
            df = self.utils.convert_ms_to_datetime(df)
        except:
            pass
        df = df.reset_index(drop=True).sort_values('usage_timeframe')
        df.index = df['usage_timeframe']
        if rename == True:
            df = df.rename(columns={'imsi_id': 'device_id'})
        return df
        
    def common_sequence(self, device_list, df_device,df_history ,distance_method:int=0.5,distance:int=100,minutes:int=5):
        result = []
        df_merged_list = []
        t1 = time.time()
        count = 0
        for device_id in device_list:
            df_cotraveler = df_history[df_history['device_id'] == device_id]

            df_main = df_device.copy()    
            tol = minutes * 60 * 10000000000000000
            df_main = self.utils.convert_date_to_mill_pandas(df_main,'usage_timeframe')

            df_merged = pd.merge_asof(left=df_main,
                                    right=df_cotraveler,
                                    on='usage_timeframe',
                                    direction='nearest',
                                    tolerance=tol).dropna()
            df_merged_list.append(df_merged)
            # if df_merged.empty:
            #     continue
            df_main = df_merged[['imsi_id',
                                    'location_latitude_x',
                                    'location_longitude_x',
                                    'usage_timeframe',
                                    'grid_x']]

            df_cotraveler = df_merged[['device_id',
                                'location_latitude_y',
                                'location_longitude_y',
                                'usage_timeframe',
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
        df_merged_list = pd.concat(df_merged_list)
        print(df_merged_list.head())
        print(f"LCS Time: {time.time() -t1}")
            
        return result  
    
    def add_visit_count_column(self,table,df_common):
        device_counts = df_common['device_id'].value_counts()
        device_counts_df = device_counts.reset_index()
        device_counts_df = device_counts.reset_index()
        device_counts_df.columns = ['device_id', 'count']
        table['device_id'] = table['device_id'].astype(str)
        table = pd.merge(table, device_counts_df, on='device_id', how='left')
        table=table.sort_values(by='count', ascending=False)
        table = table.head(10)
        return table
    
    def add_statistics_columns(self,table,df_device,df_history,minutes:int=10):
        unique_device_list = list(table['device_id'].unique())
        common_results = self.common_sequence(device_list=unique_device_list ,df_device=df_device,df_history=df_history, distance=0.5,minutes=minutes)
        common_results = pd.DataFrame(common_results, columns=['device_id', 'sequence_distance','longest_sequence'])
        table = pd.merge(table,common_results,on='device_id')
        table=table.sort_values(by=['sequence_distance', 'longest_sequence','count'], ascending=False)
        table.reset_index(drop=True, inplace=True)
        table.insert(0, 'Rank', range(1, len(table) + 1))
        return table
    
    def add_common_locations_hits_coulmn(self,table,df_common):
        df_common = self.utils.convert_datetime_to_ms(df_common,'usage_timeframe')
        df_common = df_common.groupby('device_id').apply(self.create_location_array).reset_index(name='COMMON_LOCATIONS_HITS')
        table = pd.merge(table,df_common,on='device_id')
        print(table)
        return table


    def create_location_array(self,group):
        return [list(x) for x in zip(group['location_latitude'], group['location_longitude'],group['device_id'],group['usage_timeframe'],group['location_name'],group['service_provider_id'])]


