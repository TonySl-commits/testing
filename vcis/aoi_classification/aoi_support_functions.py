import pandas as pd
from geopy.geocoders import Nominatim
from geopy.point import Point
import haversine
import re
import math

import warnings
warnings.filterwarnings('ignore')

def filter_points(points):
    modified = True
    while modified:
        modified = False
        centroids = []
        for point in points:
            lat_sum = 0
            lon_sum = 0
            count = 0
            for other_point in points:
                if point != other_point:
                    distance = haversine.haversine(point, other_point)
                    if distance <= 0.200:
                        lat_sum += other_point[0]
                        lon_sum += other_point[1]
                        count += 1
                        points.remove(other_point)
            if count > 1:
                # Calculate the center of mass of the points
                center_lat = lat_sum / count
                center_lon = lon_sum / count
                center_point = (center_lat, center_lon)
                # Add the center of mass point to the list of centroids
                centroids.append(center_point)

                modified = True             
    return points

def count_unique_dates(df, group_col):
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    grouped_df = df.groupby(group_col)
    result_dict = {}

    for group_name, group_df in grouped_df:
        unique_dates = group_df['Timestamp'].dt.date.unique()
        result_dict[group_name] = len(unique_dates)

    result_df = pd.DataFrame.from_dict(result_dict, orient='index', columns=['nbr_of_days'])
    result_df.index.name = group_col
    return result_df
    

def reverse_geocoding(point):
    geolocator = Nominatim(user_agent="test")
    try:
        location = geolocator.reverse(Point(point), language='en-US')
        location_name = location.raw['display_name']
        location_name = re.sub(r'[^\x00-\x7F]+', '', location_name)
        location_name = ", ".join([x.strip() for x in location_name.split(",") if x.strip()])
        return location_name
    except:
        return None
    
    
def analyze_suspiciousness(result_AOI, df_polygon):
    def format_coords(row):
        return '(' + str(row['LAT']) + ',' + str(row['LNG']) + ')'

    def aggregate_coords(coords):
        return list(coords)
    
    df_polygon = pd.merge(result_AOI, df_polygon, on=['NAME_ID', 'LAT', 'LNG'], how='inner') \
                    .rename(columns={'NBR_HITS_x': 'TOTAL_HITS', 'NBR_HITS_y':'NBR_HITS'})

    # Save AOI as a coordinate point
    result_AOI['LOCATION_COORDS'] = '(' + result_AOI['LAT'].astype(str) + ',' + result_AOI['LNG'].astype(str) + ')'
    
    # Group AOI point in polygon
    df_polygon['LOCATION_COORDS'] = df_polygon.apply(format_coords, axis=1)
    
    # Get Suspicious Area data 
    df_polygon[['SHAPE_ID', 'SHAPE_TYPE', 'SHAPE_NAME', 'AREA_SUSPICIOUSNESS']] = df_polygon['SHAPE'].apply(lambda x: pd.Series([x['LOCATION_ID'], str(x['TYPE']), str(x['NAME']), str(x['SUSPICIOUS'])]))
    
    # Aggregate the data for the current Name_ID   
    df = df_polygon.groupby(['SHAPE_ID', 'SHAPE_NAME', 'SHAPE_TYPE', 'AREA_SUSPICIOUSNESS', 'NAME_ID']) \
        .agg({'NBR_HITS': 'sum', 'TOTAL_HITS': 'sum', 'LOCATION_COORDS': aggregate_coords}).reset_index()

    # Get the number of AOIs inside the suspicious area
    aois_inside_sus_area = df.groupby(['SHAPE_ID', 'SHAPE_NAME', 'SHAPE_TYPE', 'AREA_SUSPICIOUSNESS', 'NAME_ID']) \
                        .size().reset_index(name='CLUSTERS_IN')
    
    df = pd.merge(df, aois_inside_sus_area, on=['SHAPE_ID', 'SHAPE_NAME', 'SHAPE_TYPE', 'AREA_SUSPICIOUSNESS', 'NAME_ID'], how='left')

    # Number of device AOIs / Total Clusters
    df['TOTAL_CLUSTERS'] =  len(result_AOI['LOCATION_COORDS']) 

    # Compute the percentage of device's AOIs in each suspicious area
    df['CLUSTERS_PERCENTAGE'] = (df['CLUSTERS_IN'] / df['TOTAL_CLUSTERS']) * 100
    
    # Compute the percentage of device's hits inside each suspicious area
    df['HITS_PERCENTAGE'] = (df['NBR_HITS'] / df['TOTAL_HITS']) * 100
    
    # Compute the suspiciousness percentage of a device in each suspicious area 
    df['SUSPICIOUSNESS_PERCENTAGE'] = (df['CLUSTERS_PERCENTAGE'] * 0.6 + df['HITS_PERCENTAGE'] * 0.4)

    # Inherit suspiciousness of an area, input by the user
    df['AREA_SUSPICIOUSNESS'] = df['AREA_SUSPICIOUSNESS'].astype(float)

    # Suspiciousness of a device from its hits and AOIs inside each suspicious area
    df['FLAGGED_AREA_CONTRIBUTION'] = df['SUSPICIOUSNESS_PERCENTAGE'] * (df['AREA_SUSPICIOUSNESS'] / 100)
    
    # Compute the total suspiciousness of a device; the sum of the flagged area contribution from a device's AOIs
    df['DEVICE_SUSPICIOUSNESS'] = df['FLAGGED_AREA_CONTRIBUTION'].mean()

    # print(df[['SHAPE_NAME','CLUSTERS_PERCENTAGE','HITS_PERCENTAGE','SUSPICIOUSNESS_PERCENTAGE','FLAGGED_AREA_CONTRIBUTION','AREA_SUSPICIOUSNESS','DEVICE_SUSPICIOUSNESS']])
    
    # Drop unnecessary columns
    df.drop(['CLUSTERS_IN', 'TOTAL_CLUSTERS', 'CLUSTERS_PERCENTAGE', 'HITS_PERCENTAGE'], axis=1, inplace=True)
    df.rename(columns={'NAME_ID': 'DEVICE_ID_GEO'}, inplace=True)
    df.rename_axis('INDEX_NUMBER', inplace=True)
    df = df.reset_index()
    
    # Remove and reorder columns properly
    df = df[['INDEX_NUMBER','DEVICE_ID_GEO','SHAPE_NAME','SHAPE_ID','SHAPE_TYPE','LOCATION_COORDS','SUSPICIOUSNESS_PERCENTAGE','FLAGGED_AREA_CONTRIBUTION','AREA_SUSPICIOUSNESS','DEVICE_SUSPICIOUSNESS']]

    # Convert these columns to float
    df['SUSPICIOUSNESS_PERCENTAGE'] = df['SUSPICIOUSNESS_PERCENTAGE'].astype(float)
    df['FLAGGED_AREA_CONTRIBUTION'] = df['FLAGGED_AREA_CONTRIBUTION'].astype(float)

    # print(df[['DEVICE_ID_GEO','SHAPE_NAME','SHAPE_TYPE','SUSPICIOUSNESS_PERCENTAGE','FLAGGED_AREA_CONTRIBUTION','AREA_SUSPICIOUSNESS','DEVICE_SUSPICIOUSNESS']])
    
    return df