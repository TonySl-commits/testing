import math
import datetime
import pandas as pd
import folium
from vcis.utils.utils import CDR_Properties,CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from collections import deque
from shapely.geometry import Point, Polygon, MultiPolygon, LineString
from shapely.ops import unary_union
import warnings
import time
class ActivityScanFunctions:
    def __init__(self, verbose=False):
        self.utils = CDR_Utils()
        self.properties = CDR_Properties()
        self.verbose = verbose


    def find_neighboring_grids(self, grids, step_lat, step_lon):
        if self.verbose:
            print("Finding neighboring grids...")
        # Function to check if two grids are neighbors
        def is_neighbor(grid1, grid2):
            return abs(grid1[0] - grid2[0]) <= step_lat and abs(grid1[1] - grid2[1]) <= step_lon

        visited = set()
        groups = []

        for grid in grids:
            grid_key = (grid['latitude_grid'], grid['longitude_grid'])
            if grid_key not in visited:
                queue = deque([(grid, grid_key)])
                visited.add(grid_key)
                current_group = [grid]

                while queue and len(current_group) < 9999:
                    current_grid, current_key = queue.popleft()
                    for neighbor in grids:
                        neighbor_key = (neighbor['latitude_grid'], neighbor['longitude_grid'])
                        if neighbor_key not in visited and is_neighbor((current_grid['latitude_grid'], current_grid['longitude_grid']), (neighbor['latitude_grid'], neighbor['longitude_grid'])):
                            queue.append((neighbor, neighbor_key))
                            visited.add(neighbor_key)
                            current_group.append(neighbor)
                            if len(current_group) ==9999:
                                break

                groups.append(current_group)
                
        return groups

    def get_polygon_list(self, groups, radius):
        if self.verbose:
            print("Creating polygon list from groups...")
        polygons_list = []  # List to store the union shapes for each group
        for group in groups:
            # Create a buffer around each point in the group
            buffers = [Point(grid['longitude_grid'], grid['latitude_grid']).buffer(radius) for grid in group]
            
            # Union all buffers within the group
            group_union = unary_union(buffers)
            polygons_list.append(group_union)  # Append the union shape of the current group

        return polygons_list

    def convert_polygon_to_df(self, neighboring_groups):
        if self.verbose:
            print("Converting polygons to DataFrame...")
        group_grids_df = pd.DataFrame(columns=['id', 'latitude_grid', 'longitude_grid'])

        for i, group in enumerate(neighboring_groups):
            flat_group = [(g['latitude_grid'], g['longitude_grid']) for g in group]
            temp_df = pd.DataFrame(flat_group, columns=['latitude_grid', 'longitude_grid'])
            temp_df['id'] = i 
            group_grids_df = pd.concat([group_grids_df, temp_df], ignore_index=True)

        return group_grids_df

    def get_min_max_df(self, group_grids_df, df_main):
        df_grids = pd.merge(group_grids_df, df_main, on=['latitude_grid', 'longitude_grid'])

        grouped = df_grids.groupby(['latitude_grid', 'longitude_grid'])

        result_df = pd.DataFrame({
            'id': grouped['id'].first(),
            'min_usage_timeframe': grouped['usage_timeframe'].min(),
            'max_usage_timeframe': grouped['usage_timeframe'].max(),

        }).reset_index()
        return result_df

    def get_groups_min_max_df(self, full_min_max_df, polygons_list):
        if self.verbose:
            print("Aggregating min and max usage timeframe for each group...")
        grouped = full_min_max_df.groupby('id')

        def list_grids(group):
            grids = sorted(list(set(zip(group['latitude_grid'], group['longitude_grid']))))
            return grids

        # Create a new DataFrame with the minimum and maximum values for each group
        final_df = pd.DataFrame({
            'id': grouped['id'].first(),
            'location_grids': grouped.apply(list_grids),
            'min_usage_timeframe': grouped['min_usage_timeframe'].min(),
            'max_usage_timeframe': grouped['max_usage_timeframe'].max(),
        }).reset_index(drop=True)

        return final_df

    def nodes_scan_query(self, polygon):
        if self.verbose:
            print("Building node scan query...")
        query = self.properties.polygon_activity_scan_1
        polygon = str(polygon)
        query = query.replace('table_name', str(self.properties.lebanon_nodes_table_name))
        query = query.replace('index', str(self.properties.lebanon_nodes_table_name + '_idx01'))
        query = query.replace('replace_polygon', polygon)
        return query

    def device_scan_query_builder(self, start_date, end_date, region, sub_region, polygon):
        query = self.properties.polygon_activity_scan_with_time
        date = datetime.datetime.fromtimestamp(start_date / 1000)
        year = str(date.year)
        month = str(date.month)
        start_date = str(start_date)
        end_date = str(end_date)
        region = str(region)
        sub_region = str(sub_region)

        query = query.replace('year', year)
        query = query.replace('month', month)
        query = query.replace('replace_start_date', start_date)
        query = query.replace('replace_end_date', end_date)
        query = query.replace('region', region)
        query = query.replace('subre', sub_region)
        query = query.replace('replace_polygon', str(polygon))
        return query

    def activity_scan(self, data, session=None, region='142', sub_region='145'):
        if self.verbose:
            print("Starting activity scan...")
        x = 0
        data_retrived = 0
        df_list = []

        for index, row in data.iterrows():
            df = pd.DataFrame()
            
            query = self.device_scan_query_builder(start_date=row['min_usage_timeframe'],
                                                end_date=row['max_usage_timeframe'],
                                                polygon=row['polygon'],
                                                region=region,
                                                sub_region=sub_region)
            try:
                df1 = session.execute(query)
                df = pd.DataFrame(df1.current_rows)

            except Exception as e:
                if self.verbose:
                    print(f"Exception at index {index}: {e}")
                continue

            if len(df) != 0:
                df = df[['device_id',
                        'location_latitude',
                        'location_longitude',
                        'usage_timeframe',
                        'location_name',
                        'service_provider_id']].drop_duplicates()

                df = self.utils.convert_ms_to_datetime(df)
                data_retrived += len(df)
                df_list.append(df)

            if len(df) == 0:
                x += 1
            if self.verbose:
                print(f"★★★★★★★★★★ Requests remaining: {len(data)-index:<4} ★★★★★★★★★★ Empty Requests: {x:<4} ★★★★★★★★★★ Data Retrived: {data_retrived:<6} ★★★★★★★★★★")
        if self.verbose:
            print(f'Empty requests count: {x}')
        common_df = pd.concat(df_list)
        return common_df
