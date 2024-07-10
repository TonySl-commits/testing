import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.cluster import DBSCAN
from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from pyproj import Geod
from shapely import Polygon
from scipy.spatial.distance import cdist
import sys

pd.options.mode.chained_assignment = None  # default='warn'
sys.setrecursionlimit(10**6)

class CoexistenceFunctions():
    def __init__(self):
        self.properties = CDR_Properties()
        self.utils = CDR_Utils()
        self.tools = CassandraTools()
    
    def device_scan_query_builder(self,  start_date, end_date , scan_distance, latitude, longitude):
        query = self.properties.device_scan_query
        date = datetime.fromtimestamp(end_date / 1000)

        year = str(date.year)
        month = str(date.month)
        lat = str(latitude)
        longitude = str(longitude)
        start_date = str(start_date)
        end_date = str(end_date)
        scan_distance = str(scan_distance)
        query = query.replace('year', year)
        query = query.replace('month', month) 
        query = query.replace('value1', lat)
        query = query.replace('value2', longitude)
        query = query.replace('value3', start_date)
        query = query.replace('value4', end_date)
        query = query.replace('scan_distance', scan_distance)
        # print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n',query)
        return query

    def get_common_df(self, data,scan_distance:int=200 ,passed_server:str='10.1.10.110',default_timeout:int=100000,default_fetch_size:int=300000):
        session = self.tools.get_cassandra_connection(passed_server,
                                                  default_timeout=default_timeout,
                                                  default_fetch_size=default_fetch_size)

        # time_begin, time_end = self.calculate_timeframe(data)
        
        # time_begin = 1697347499
        # time_end = 1698558590
        
        # from vcis.utils.utils import CDR_Utils
        # utils = CDR_Utils()
        # time_begin = "2023-01-01"    
        # time_end = "2023-12-30"
        # time_begin = str(time_begin)
        # time_end = str(time_end)
        # time_begin = utils.convert_date_to_timestamp_str(time_begin)
        # time_end = utils.convert_date_to_timestamp_str(time_end)

        time_begin = 1697347498000
        time_end = 1698558591000
        
        unique_grids = data["grid"].unique()

        df_list = []
        x = 0
        for grid in unique_grids:
            df = pd.DataFrame()
            grid_latitude, grid_longitude = map(float, grid.split(','))
            query = self.device_scan_query_builder(start_date = time_begin,
                                                    end_date = time_end,
                                                    scan_distance = scan_distance,
                                                    latitude = grid_latitude,
                                                    longitude = grid_longitude)
            
            df1 = session.execute(query)
            df = pd.DataFrame(df1.current_rows)
            if len(df) != 0:
                df = df[['device_id',
                        'location_latitude',
                        'location_longitude',
                        'usage_timeframe']].drop_duplicates()
                
                df.loc[:,'grid_coords'] =  grid
                df = self.utils.convert_timestamp_to_date(df)
                df_list.append(df)  
            
            # common_df = pd.concat(df_list)
            if df_list:
                common_df = pd.concat(df_list)
            else:
                common_df = pd.DataFrame()
            
            x += 1
            print("★★★★★★★★★★ Requests number: {:<3}/{:<4}★★★★★★★★★★ Data Retrieved:{:<7}"\
                .format(x, len(unique_grids),df.shape[0]))
            
        common_df = common_df.drop_duplicates(subset=['device_id','location_latitude','location_longitude','usage_timeframe'])
        return common_df      
    

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
    
    def exclude_existing_devices(self, df_history, time_begin):
        df_history['usage_timeframe'] = pd.to_datetime(df_history['usage_timeframe'], unit='ms')
        min_start_times = df_history.groupby('device_id')['usage_timeframe'].min()
        time_begin_ms = pd.to_datetime(time_begin, unit='ms')
        valid_devices = min_start_times[min_start_times >= time_begin_ms].index.tolist()
        filtered_df_history = df_history[df_history['device_id'].isin(valid_devices)]
    
        return filtered_df_history

    def calculate_timeframe(self, data):
        min_start_time = data['usage_timeframe'].min()
        max_end_time = data['usage_timeframe'].max()
        time_difference = max_end_time - min_start_time
        begin = min_start_time + time_difference
        end = max_end_time + time_difference

        begin = int(begin.timestamp() * 1000)
        end = int(end.timestamp() * 1000)

        return begin, end
    
    def filter(self, df_device, df_history):
        main_device_grid_coords = df_device['grid'].to_list()
        all_devices_grid_coords = df_history.groupby('device_id')['grid'].apply(list)
        device_ids = all_devices_grid_coords.index.tolist()

        for coexistor_device_grid_coords, id in zip(all_devices_grid_coords, device_ids):
            grid_similarity_ratio = len(list(set(main_device_grid_coords) & set(coexistor_device_grid_coords)))/len(list(set(main_device_grid_coords)))
            hit_count_ratio = len(df_history[df_history['device_id'] == id]) /len(df_device)
            if grid_similarity_ratio < 0.2 or hit_count_ratio < 0.4:
                df_history = df_history[df_history['device_id'] != id]
        return df_history
    
    # def clustering(self,df):
    #     list_of_devices = df['device_id'].unique().tolist()
    #     clustered_df = pd.DataFrame(columns=df.columns)

    #     for device in list_of_devices:
    #         device_history = df[df['device_id'] == device]
    #         device_history = device_history[['device_id','location_latitude', 'location_longitude','usage_timeframe']]
    #         hit_coords = device_history[['location_latitude', 'location_longitude']].values
    #         hit_coords = np.array(hit_coords)
    #         clustering = DBSCAN(eps=0.5/6371, min_samples=10, metric='haversine').fit(hit_coords)
    #         labels = clustering.labels_
    #         device_history['cluster_labels'] = labels

    #         centroid_df = pd.DataFrame(columns=device_history.columns)

    #         for label in device_history['cluster_labels'].unique():
    #             cluster_rows = device_history[device_history['cluster_labels'] == label]
                
    #             if label != -1:
    #                 centroid_coords = cluster_rows[['location_latitude', 'location_longitude']].mean()
                    
    #                 centroid_row = pd.Series(centroid_coords)
    #                 centroid_row['cluster_labels'] = label
    #                 centroid_row['device_id'] = device
    #                 centroid_row['usage_timeframe'] = cluster_rows['usage_timeframe'].mean()
    #                 centroid_df = pd.concat([centroid_df, centroid_row.to_frame().T], ignore_index=True)
    #             else:
    #                 centroid_df = pd.concat([centroid_df, cluster_rows], ignore_index=True)
                    
    #         clustered_df = pd.concat([clustered_df, centroid_df], ignore_index=True)
    #         clustered_df = clustered_df[['device_id','location_latitude', 'location_longitude','usage_timeframe','cluster_labels','grid']]

    #     return clustered_df

    def clustering(self, data):
        list_of_devices = data['device_id'].unique().tolist()
        clustered_df = pd.DataFrame(columns=data.columns)

        for device in list_of_devices:
            device_history = data[data['device_id'] == device]
            device_history = self.utils.convert_datetime_to_ms(device_history)
            dbscan = DBSCAN(eps=1.5/6371, min_samples=20) 
            clusters = dbscan.fit_predict(np.array(device_history[['location_latitude', 'location_longitude']]))
            device_history['Clusters'] = clusters
            unique_clusters = list(set(clusters))
            if -1 in unique_clusters:
                unique_clusters.remove(-1)
                
            if len(unique_clusters) != 0:
                max_cluster_value = max(unique_clusters)
                for cluster_label in unique_clusters:
                    cluster = device_history[device_history['Clusters'] == cluster_label]
                    device_history = device_history[device_history['Clusters'] != cluster_label]
                    cluster = cluster.reset_index(drop=True)
                    time = np.array(cluster['usage_timeframe'])
                    time_diffs = np.diff(time)
                    greater = time_diffs > np.timedelta64(7200000, 'ms')
                    for index, time_diff in enumerate(greater):

                        if time_diff:
                            new_cluster_value = max_cluster_value + 1
                            max_cluster_value = new_cluster_value
                            cluster.loc[index+1:, 'Clusters'] = new_cluster_value

                    device_history = pd.concat([device_history, cluster])
                    device_history.reset_index(drop=True, inplace=True)
            centroid_df = pd.DataFrame(columns=data.columns)

            for label in device_history['Clusters'].unique():
                cluster_rows = device_history[device_history['Clusters'] == label]

                if label != -1:
                    centroid_coords = cluster_rows[['location_latitude', 'location_longitude']].mean()

                    centroid_row = pd.Series(centroid_coords)
                    centroid_row['Clusters'] = label
                    centroid_row['device_id'] = device
                    centroid_row['usage_timeframe'] = cluster_rows['usage_timeframe'].mean()
                    centroid_df = pd.concat([centroid_df, centroid_row.to_frame().T], ignore_index=True)
                else:
                    centroid_df = pd.concat([centroid_df, cluster_rows], ignore_index=True)

            clustered_df = pd.concat([clustered_df, centroid_df], ignore_index=True)
            clustered_df = clustered_df[['device_id','location_latitude', 'location_longitude','usage_timeframe','Clusters','grid']]

        return clustered_df
    
    def haversine_distance(self,coord1, coord2):
        R = 6371  
        lat1, lon1 = np.radians(coord1)
        lat2, lon2 = np.radians(coord2)
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
        distance = R * c
        return distance


    def compute_dany_distance(self,curve1, curve2):
        matrix_distance = np.array(cdist(curve1, curve2,metric = self.haversine_distance))
        row_min_values = np.min(matrix_distance, axis=1)
        sum = np.sum(row_min_values)
        return sum
    
    
    # def compute_frechet_distance(self,curve1, curve2):
    #     def _c(ca, i, j, p1, p2):
    #         # print("ca: ",ca,"i: ",i,"j: ",j,"p1: ",p1,"p2: ",p2)
    #         if ca[i, j] > -1:
    #             return ca[i, j]
    #         elif i == 0 and j == 0:
    #             ca[i, j] = self.haversine_distance(p1[0], p2[0])
    #         elif i > 0 and j == 0:
    #             ca[i, j] = max(_c(ca, i - 1, 0, p1, p2), self.haversine_distance(p1[i], p2[0]))
    #         elif i == 0 and j > 0:
    #             ca[i, j] = max(_c(ca, 0, j - 1, p1, p2), self.haversine_distance(p1[0], p2[j]))
    #         elif i > 0 and j > 0:
    #             ca[i, j] = max(
    #                 min(
    #                     _c(ca, i - 1, j, p1, p2),
    #                     _c(ca, i - 1, j - 1, p1, p2),
    #                     _c(ca, i, j - 1, p1, p2)
    #                 ),
    #                 self.haversine_distance(p1[i], p2[j])
    #             )
    #         else:
    #             ca[i, j] = float('inf')
    #         print(ca[i, j])
    #         return ca[i, j]
    #     n = len(curve1)
    #     m = len(curve2)
    #     if n == 0 or m == 0:
    #         raise ValueError("Input curves must not be empty.")

    #     ca = np.ones((n, m)) * -1
    #     return _c(ca, n - 1, m - 1, curve1, curve2)

    def metrics(self, df_history, df_common, df_device):
        # Get percentage metric
        df_history_grouped = df_history.groupby('device_id').count().reset_index()\
            [['device_id','usage_timeframe']]
        df_common_grouped = df_common.groupby('device_id').count().reset_index()\
            [['device_id','usage_timeframe']]
        
        df_merged = pd.merge(df_common_grouped, df_history_grouped,on='device_id')
        df_merged.rename(columns={'usage_timeframe_x': 'common_record' , 'usage_timeframe_y': 'history_record'}, inplace=True) 
        df_merged['percentage'] = (df_merged['common_record']/df_merged['history_record'])*100
        df_metric = df_merged.sort_values(by='percentage', ascending=False)
        
        # Get area and distance
        df_history_clustered = self.clustering(df_history)
        df_device_clustered = self.clustering(df_device)
        # df_device_clustered['usage_timeframe'] = df_device_clustered['usage_timeframe'].astype('int64') // 10**6
        df_device_clustered = df_device_clustered.sort_values(by="usage_timeframe", axis=0)      
        list_of_devices = df_history_clustered['device_id'].unique().tolist() 
        df_device_coords = np.array(df_device_clustered[['location_latitude', 'location_longitude']].values.tolist())
        

        for device in list_of_devices:
            print("DEVICE: ", device)
            df_coexistor = df_history_clustered[df_history_clustered['device_id'] == device]
            df_coexistor = df_coexistor.sort_values(by="usage_timeframe", axis=0, ascending=False)
            df_coexistor_coords = np.array(df_coexistor[['location_latitude', 'location_longitude']].values.tolist())
            coords = np.concatenate((df_device_coords, df_coexistor_coords), axis=0)
            polygon = Polygon(coords)
            geod = Geod(ellps="WGS84")
            poly_area, poly_perimeter = geod.geometry_area_perimeter(polygon)
            distance = self.compute_dany_distance(df_coexistor_coords,df_device_coords)
            distance_percent = 100/((distance/75)+1)
            index_of_device_id = df_metric.loc[df_metric['device_id'] == device].index[0]
            df_metric.at[index_of_device_id, 'Distance percentage'] = distance_percent
            # df_metric.at[index_of_device_id, 'Area Difference'] = abs(poly_area)

        df_metric['Confidence'] = 0.65*df_metric['percentage'] + 0.35*df_metric['Distance percentage']
        df_metric = df_metric.sort_values(by='Confidence', ascending=False)
        return df_metric