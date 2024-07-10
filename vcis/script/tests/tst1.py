from collections import deque
import matplotlib.pyplot as plt
import pandas as pd
from vcis.utils.utils import CDR_Properties, CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
import math
import folium
from shapely.geometry import Point
from shapely.ops import unary_union
import numpy as np
import time

#disable warnings


t1 = time.time()
properties = CDR_Properties()
utils = CDR_Utils()
cassandra_tools = CassandraTools(verbose= True)
def find_neighboring_grids(grids, step_lat, step_lon):
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

            while queue:
                current_grid, current_key = queue.popleft()
                for neighbor in grids:
                    neighbor_key = (neighbor['latitude_grid'], neighbor['longitude_grid'])
                    if neighbor_key not in visited and is_neighbor((current_grid['latitude_grid'], current_grid['longitude_grid']), (neighbor['latitude_grid'], neighbor['longitude_grid'])):
                        queue.append((neighbor, neighbor_key))
                        visited.add(neighbor_key)
                        current_group.append(neighbor)

            groups.append(current_group)

    return groups

def get_polygon_list(groups, radius):
    """Buffer all points within each group and then union these buffered areas, returning a list of union shapes."""
    polygons_list = []  # List to store the union shapes for each group
    for group in groups:
        # Create a buffer around each point in the group
        buffers = [Point( grid['longitude_grid'],grid['latitude_grid']).buffer(radius) for grid in group]
        
        # Union all buffers within the group
        group_union = unary_union(buffers)
        polygons_list.append(group_union)  # Append the union shape of the current group
    
    return polygons_list


def convert_polygon_to_df(neighboring_groups,polygons_list):
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

    # Create a new DataFrame with the minimum and maximum values for each group
    final_df = pd.DataFrame({
        'id': grouped['id'].first(),
        'location_grids': grouped.apply(list_grids),
    }).reset_index(drop=True)
    final_df['polygon'] = polygons_list
    return final_df

def get_min_max_df(group_grids_df,df_main):
    df_grids = pd.merge(group_grids_df,df_main, on=['latitude_grid','longitude_grid'])

    grouped = df_grids.groupby(['latitude_grid','longitude_grid'])

    result_df = pd.DataFrame({
        'id': grouped['id'].first(),
        'min_usage_timeframe': grouped['usage_timeframe'].min(),
        'max_usage_timeframe': grouped['usage_timeframe'].max(),

    }).reset_index()
    return result_df

def get_groups_min_max_df(full_min_max_df):
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

def nodes_scan_query(polygon):
    query = properties.polygon_activity_scan_1
    polygon = str(polygon)
    query = query.replace('table_name', str(properties.lebanon_nodes_table_name))
    query = query.replace('index', str(properties.lebanon_nodes_table_name + '_idx01'))
    query = query.replace('replace_polygon',polygon)
    return query

def get_close_nodes(data,session=None):
    x =0
    data_retrived = 0 
    df_list = []
    id=0            
    for i, row in data.iterrows():
        df = pd.DataFrame()
        polygon = row['polygon']
        query = nodes_scan_query(polygon = polygon)

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
        # print("★★★★★★★★★★ Getting Close Nodes ★★★★★★★★★★ GPS Scans remaining: {:<4} ★★★★★★★★★★ Data Retrived: {:<6} ★★★★★★★★★★"\
        #     .format(len(grouped)-id,  data_retrived))
    print('★★★★★★★★★★ empty: {:<4} ★★★★★★★★★★'.format(x))
    df_near_nodes = pd.concat(df_list)
    return df_near_nodes


def visualize(neighboring_grids,polygons_list,df_main):
    first_group_first_point = neighboring_grids[0][0]
    base_map_location = (first_group_first_point['latitude_grid'], first_group_first_point['longitude_grid'])
    m = folium.Map(location=base_map_location, zoom_start=13)

    # Add each unioned shape to the map
    for i, union_shape in enumerate(polygons_list):

        exterior_coords_tuples = list(union_shape.exterior.coords)

        # Create a Folium Polygon layer for the unioned shape
        folium_polygon = folium.Polygon(locations=exterior_coords_tuples, color="blue", fill=True, fill_color="rgba(255, 0, 0, 0.05)", weight=2, opacity=0.8)

        # Add the Polygon layer to the map
        folium_polygon.add_to(m)

    for index, location_info in df_main.iterrows():
        folium.Circle(
            [location_info["latitude_grid"], location_info["longitude_grid"]],
            radius=3,
        ).add_to(m)

    m.show_in_browser()
    m.save('a.html')


df_main = pd.read_csv(properties.passed_filepath_excel + 'df_main.csv')

df_main = utils.binning(df_main,30)
df_filtered = df_main.drop_duplicates(subset=['latitude_grid','longitude_grid'])
df_filtered = df_filtered.sort_values(['latitude_grid','longitude_grid'])
grids = df_filtered[['latitude_grid', 'longitude_grid']].to_dict('records')

step_lat,step_lon = utils.get_step_size(30*math.sqrt(2))
neighboring_grids= find_neighboring_grids(grids, step_lat, step_lon)
polygons_list = get_polygon_list(neighboring_grids, step_lat)

print(neighboring_grids)
print(len(polygons_list))
neighboring_grids_df = convert_polygon_to_df(neighboring_grids,polygons_list)
print(neighboring_grids_df)

# visualize(neighboring_grids,polygons_list,df_main)

# full_min_max_df = get_min_max_df(neighboring_grids_df,df_main)
# groups_min_max_df = get_groups_min_max_df(full_min_max_df)

session = cassandra_tools.get_cassandra_connection(server="10.1.2.205")
a = get_close_nodes(neighboring_grids_df,session)

print(a)
t2 = time.time()
print(t2-t1)








