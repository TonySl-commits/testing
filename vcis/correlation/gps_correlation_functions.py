from vcis.utils.properties import CDR_Properties
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
ignore_warnings = True

import numpy as np
import pandas as pd
import json
import math
from collections import deque
from shapely.geometry import Point, Polygon, MultiPolygon
from shapely.ops import unary_union


class GPSCorrelationFunctions:
    def __init__(self, verbose=False):
        self.properties = CDR_Properties()
        self.verbose = verbose
        self.cassandra_tools = CassandraTools()
        self.cassandra_spark_tools = CassandraSparkTools()
    def get_close_nodes(self,
                        data,
                        distance:int=30,session=None):

        x =0
        data_retrived = 0 
        df_list = []
        id=0            
        for i, row in data.iterrows():
            df = pd.DataFrame()
            polygon = row['polygon']
            query = self.nodes_scan_query(polygon = polygon)

            try:
                df1 = session.execute(query)
                df = pd.DataFrame(df1.current_rows)
                df['polygon'] =polygon
                df['scan_id'] = id  
            except Exception as e:
                print(e)
                continue

            data_retrived+=len(df)
            df_list.append(df)

            if len(df) == 0 :
                x += 1
            id+=1

            print("★★★★★★★★★★ Getting Close Nodes ★★★★★★★★★★ GPS Scans remaining: {:<4} ★★★★★★★★★★ Data Retrieved: {:<6} ★★★★★★★★★★"\
                .format(len(grouped)-id,  data_retrived))
        print('★★★★★★★★★★ empty: {:<4} ★★★★★★★★★★'.format(x))
        df_near_nodes = pd.concat(df_list)
        return df_near_nodes

    def convert_polygon_to_df(self,neighboring_groups,polygons_list):
        group_grids_df = pd.DataFrame(columns=['id', 'latitude_grid', 'longitude_grid'])

        for i, group in enumerate(neighboring_groups):
            flat_group = [(g['latitude_grid'], g['longitude_grid']) for g in group]
            temp_df = pd.DataFrame(flat_group, columns=['latitude_grid', 'longitude_grid'])
            temp_df['id'] = i 
            group_grids_df = pd.concat([group_grids_df, temp_df], ignore_index=True)
        grouped = group_grids_df.groupby('id')

        def list_grids(group):
            grids = sorted(list(set(zip(group['latitude_grid'], group['longitude_grid']))))
            return grids

        final_df = pd.DataFrame({
            'id': grouped['id'].first(),
            'location_grids': grouped.apply(list_grids),
        }).reset_index(drop=True)
        final_df['polygon'] = polygons_list
        return final_df
    
    # def get_close_nodes(self,
    #                     data,
    #                     distance:int=30,session=None):

    #     x =0
    #     data_retrived = 0 
    #     df_list = []
    #     id=0            
    #     scan_distance = int(distance + distance/2*math.sqrt(2))
    #     grouped = data.groupby('grid')
    #     for name, group in grouped:
    #         df = pd.DataFrame()
    #         row = group.iloc[0]
    #         latitude = row['latitude_grid']
    #         longitude = row['longitude_grid']
    #         query = self.nodes_scan_query(latitude=latitude,longitude=longitude,distance=scan_distance)

    #         try:
    #             df1 = session.execute(query)
    #             df = pd.DataFrame(df1.current_rows)
    #             df['latitude_grid'] = latitude
    #             df['longitude_grid'] = longitude
    #             df['scan_id'] = id  
    #         except Exception as e:
    #             if self.verbose:
    #                 print(e)
    #             continue

    #         data_retrived+=len(df)
    #         df_list.append(df)

    #         if len(df) == 0 :
    #             x += 1
    #         id+=1

    #         print("★★★★★★★★★★ Getting Close Nodes ★★★★★★★★★★ GPS Scans remaining: {:<4} ★★★★★★★★★★ Data Retrived: {:<6} ★★★★★★★★★★"\
    #             .format(len(grouped)-id,  data_retrived))
    #     print('★★★★★★★★★★ empty: {:<4} ★★★★★★★★★★'.format(x))
    #     df_near_nodes = pd.concat(df_list)
    #     df_near_nodes.rename(columns={'location_latitude':'node_latitude',
    #                             'location_longitude': 'node_longitude'},inplace=True)
    #     df_near_nodes = pd.merge(data,df_near_nodes, on=['latitude_grid','longitude_grid'])
    #     df_near_nodes.rename(columns={'location_latitude':'gps_latitude',
    #                             'location_longitude': 'gps_longitude'},inplace=True)
    #     df_near_nodes = df_near_nodes.sort_values('scan_id')
        
    #     return df_near_nodes
    

    def nodes_scan_query(self,polygon):
        query = self.properties.polygon_activity_scan_1
        polygon = str(polygon)
        query = query.replace('table_name', str(self.properties.lebanon_nodes_table_name))
        query = query.replace('index', str(self.properties.lebanon_nodes_table_name + '_idx01'))
        query = query.replace('replace_polygon',polygon)
        return query
    

    def get_possible_cgi_id(self,df_gps_nodes,session=None):
        df_list = []
        data_retrived=0
        count= 0 
        x=0
        df_osmid = pd.DataFrame()      
        # filtered_df_gps_nodes = df_gps_nodes[df_gps_nodes['extremity'] == 1]
        # unique_osmid = filtered_df_gps_nodes['osmid'].unique()
        unique_osmid  = df_gps_nodes['osmid'].unique()
        query = self.properties.bts_node_query

        osmid_sublists = np.array_split(unique_osmid, len(unique_osmid) //  1000 + (len(unique_osmid) %  1000 !=  0))

        for osmid_sublist in osmid_sublists:
            df = pd.DataFrame()
            # Convert the sublist to a JSON array string
            osmid_list_str = ', '.join(map(lambda x: f'"{x}"', osmid_sublist))
            query = query.replace('replace_osmid_list',str(osmid_list_str))      

            try:
                df1 = session.execute(query)
                df = pd.DataFrame(df1.current_rows)
                
            except Exception as e:
                if self.verbose:
                    print(f"Something Went Wrong: {e}")

            data_retrived+=len(df)
            df_list.append(df)

            if len(df) == 0 :
                x += 1
                continue
            print("★★★★★★★★★★ Getting Possible CGIs ★★★★★★★★★★ Osmid Lists remaining: {:<4} ★★★★★★★★★★ Data Retrieved: {:<6} ★★★★★★★★★★"\
                .format(len(osmid_sublists)-count,  data_retrived))
            count+=1
        df_osmid = pd.concat(df_list)
        df_gps_nodes = pd.merge(df_gps_nodes,df_osmid, on='osmid')
        return df_gps_nodes
    

    def get_bts_location(self,df_gps_nodes,session,server:str='10.1.10.66'):
        
        unique_cgi = df_gps_nodes['cgi_id'].unique()
        bts_table = self.cassandra_tools.get_bts_table(session=session,server=server)
        print(bts_table)
        df_cgi = bts_table[bts_table['cgi_id'].isin(unique_cgi)]
        df_cgi= df_cgi[['cgi_id','location_latitude','location_longitude','location_azimuth']]
        df_cgi['location_azimuth'] = df_cgi['location_azimuth'].replace(to_replace=r'(?i)indoor', value=360, regex=True).astype(float)

        df_cgi.rename(columns={'location_latitude':'bts_latitude',
                                    'location_longitude': 'bts_longitude',
                                    'location_azimuth':'bts_azimuth'},inplace=True)
        df_gps_nodes = pd.merge(df_gps_nodes,df_cgi, on='cgi_id')
        return df_gps_nodes
    
    def get_connected_devices(self,df_gps_nodes,session=None):
        df_list = []
        x = 0
        data_retrived=0

        grouped = df_gps_nodes.groupby('osmid')
        ln = len(grouped)
        for index, group in grouped:
            query = self.properties.connected_to_bts_query
            cgi_list = group['cgi_id'].unique()
            cgi_list = json.dumps(cgi_list.tolist())  
            query = query.replace('replace_list_cgis', cgi_list)
            query = query.replace('replace_lower_time',str(group['usage_timeframe'].min()-3600000))
            query = query.replace('replace_upper_time',str(group['usage_timeframe'].max()+3600000))

            # query = query.replace('replace_lower_time',"0")
            # query = query.replace('replace_upper_time',"100000000000000000000")
            try:
                df1 = session.execute(query)
                df = pd.DataFrame(df1.current_rows)
            except Exception as e:
                if self.verbose:
                    print(f"Something Went Wrong:{e}")
                continue
            df_list.append(df)
            if len(df) == 0 :
                x += 1
            data_retrived+=len(df)
            print("★★★★★★★★★★ Getting Connected Devices ★★★★★★★★★★ Nodes remaining: {:<4}  ★★★★★★★★★★ Data Retrieved: {:<6} ★★★★★★★★★★"\
                .format(ln, data_retrived))
            ln = ln-1
        print('★★★★★★★★★★ empty: {:<4} ★★★★★★★★★★'.format(x))
        df_connected = pd.concat(df_list)
        
        return df_connected
    

    ############################
    #OPTIMIZATION FUNCTIONS