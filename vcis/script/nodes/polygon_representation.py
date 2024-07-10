from ast import literal_eval
import pandas as pd
import numpy as np
from haversine import haversine, Unit
from scipy.spatial.distance import cdist
import math
import time

from ast import literal_eval
from vcis.utils.properties import CDR_Properties
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools

class BTS_PolygonSector:
    def __init__(self):
        self.data = None
        self.original_data = None
        self.cassandra_spark_tools = CassandraSparkTools()
        self.properties = CDR_Properties()
    
    def read_and_process_data_from_cassandra(self, passed_connection_host : str = "10.1.10.66"):
        data = self.cassandra_spark_tools.get_spark_data(self.properties.bts_table_sector_angles, passed_connection_host = passed_connection_host)
        data = data.toPandas()
         
        print(f'INFO:  BTS Data Loaded.')

        data['location_latitude'] = data['location_latitude'].astype(float)
        data['location_longitude'] = data['location_longitude'].astype(float)

        # Convert location azimuth to a numeric value
        data['location_azimuth'] = pd.to_numeric(data['location_azimuth'], errors='coerce')

        # Create a new column with tuples of latitude and longitude
        data['tower'] = list(zip(data['location_latitude'], data['location_longitude']))

        data.dropna(subset=['location_azimuth', 'left_angle', 'right_angle'], inplace=True)

        print(f'INFO:  BTS Data Processed.')

        self.original_data = data.copy() 
        self.data = data
        return data

    def average_distance_between_close_bts_towers(self, data):
        # Define a function to find n closest towers for a tower
        def find_closest_towers(row, n):
            distances = row.nsmallest(n+1)[1:]  # Exclude self
            closest_tower_coords = distances.index
            closest_towers = [tuple(coord) for coord in closest_tower_coords]
            return closest_towers

        # Calculate distances per service_provider_id
        results = []
        for provider_id, group in data.groupby('service_provider_id'):
            # Extract unique coordinates for each service_provider_id
            unique_coords = group['tower'].unique().tolist()

            # Calculate distances between all pairs of coordinates using cdist with haversine distance
            distance_matrix = cdist(unique_coords, unique_coords, metric=haversine)

            # Create a new dataframe with the distance matrix
            distance_df = pd.DataFrame(distance_matrix, index=unique_coords, columns=unique_coords)

            # Number of closest towers to consider
            n = 10 

            # Apply the function to each row of the distance dataframe
            closest_towers = distance_df.apply(find_closest_towers, axis=1, n=n)

            # Compute average distance for each tower
            average_distances = distance_df.apply(lambda row: row.nsmallest(n+1)[1:].mean(), axis=1)

            # Create a new dataframe with the results
            distances_df = pd.DataFrame({'closest_towers': closest_towers, 'average_distance': average_distances})
            distances_df.reset_index(inplace=True)
            distances_df.rename(columns={'index':'tower'}, inplace=True)
            distances_df['service_provider_id'] = provider_id  # Add service_provider_id column
            results.append(distances_df)

        # Concatenate the results for each service_provider_id
        bts_distances_df = pd.concat(results, ignore_index=True)
        print(bts_distances_df)

        return bts_distances_df
    
    def save_avg_distance_in_bts_data(self, distances_df):
        result = pd.merge(self.original_data, distances_df, how='inner', on=['service_provider_id', 'tower'])
        return result
    
    def front_polygon(self, row, refinement_level):
        center = row['tower']
        l = row['left_angle']
        r = row['right_angle']
        radius = row['average_distance'] * 1000

        if (l >= 180) and (r <= 180):
            start_angle = (360 - l)
            end_angle = (- r)

        elif (l >= 180) and (r >= 180):
            l, r = min(l, r), max(l, r)
            start_angle = (360 - l)
            end_angle = (360 - r)

        elif (l <= 180) and (r <= 180):
            l, r = min(l, r), max(l, r)
            start_angle = (- l)
            end_angle = (- r)

        elif (l <= 180) and (r >= 180):
            start_angle = (360 - l)
            end_angle = (360 - r)

        # Generate points
        angles = np.linspace(start_angle, end_angle, refinement_level)
        angles_radians = np.radians((angles + 90) % 360)

        dx = radius * np.cos(angles_radians)
        dy = radius * np.sin(angles_radians)

        latitudes = center[0] + (180 / math.pi) * (dy / 6378137)
        longitudes = center[1] + (180 / math.pi) * (dx / 6378137) / np.cos(center[0] * math.pi / 180)

        latitudes, longitudes = np.round(latitudes, 4), np.round(longitudes, 4)

        polygon = list(zip(latitudes, longitudes))
        polygon.insert(0, center)

        polygon = [list(point) for point in polygon]
        polygon.append(polygon[0])

        return polygon
    
    def circle_polygon(self, row):
        center = row['tower']
        radius = 100

        # Create an array of angles
        angles = np.arange(0, 360, 30)
        angles_radians = np.radians(angles)

        dx = radius * np.cos(angles_radians)
        dy = radius * np.sin(angles_radians)

        # Compute latitudes and longitudes
        latitudes = center[0] + (180 / math.pi) * (dy / 6378137)
        longitudes = center[1] + (180 / math.pi) * (dx / 6378137) / np.cos(center[0] * math.pi / 180)

        latitudes, longitudes = np.round(latitudes, 4), np.round(longitudes, 4)

        # Combine latitudes and longitudes
        circle = list(zip(latitudes, longitudes))
        circle = [list(point) for point in circle]

        # Close the circle by repeating the first point at the end
        circle.append(circle[0])

        return circle

    # def back_polygon(self, row, refinement_level):
    #     center = row['tower']
    #     l = row['left_angle']
    #     r = row['right_angle']
    #     radius = 100

    #     l = (l + 180) % 360
    #     r =  (r + 180) % 360

    #     if (l >= 180) and (r <= 180):
    #         start_angle = (360 - l)
    #         end_angle = (- r)

    #     elif (l >= 180) and (r >= 180):
    #         l, r = min(l, r), max(l, r)
    #         start_angle = (360 - l)
    #         end_angle = (360 - r)

    #     elif (l <= 180) and (r <= 180):
    #         l, r = min(l, r), max(l, r)
    #         start_angle = (- l)
    #         end_angle = (- r)

    #     elif (l <= 180) and (r >= 180):
    #         start_angle = (360 - l)
    #         end_angle = (360 - r)

    #     # Generate points
    #     angles = np.linspace(start_angle, end_angle, refinement_level)
    #     angles_radians = np.radians((angles + 90) % 360)

    #     dx = radius * np.cos(angles_radians)
    #     dy = radius * np.sin(angles_radians)

    #     latitudes = center[0] + (180 / math.pi) * (dy / 6378137)
    #     longitudes = center[1] + (180 / math.pi) * (dx / 6378137) / np.cos(center[0] * math.pi / 180)

    #     polygon = list(zip(latitudes, longitudes))
    #     polygon.insert(0, center)
    #     polygon.append(center)

    #     return polygon
    
    def compute_polygon_sectors(self, data, refinement_level = 10):
        data['front_polygon'] = data.apply(lambda row: self.front_polygon(row, refinement_level=refinement_level), axis=1)
        data['back_polygon'] = data.apply(lambda row: self.circle_polygon(row), axis=1)

        data['front_polygon'] = data['front_polygon'].astype(str)
        data['back_polygon'] = data['back_polygon'].astype(str)

        return data
    
    def save_results_in_cassandra(self, data, passed_table_name : str, passed_connection_host: str = '10.1.10.66'):  
        print(data.info())
        print(data[['front_polygon', 'back_polygon']])
        df = self.cassandra_spark_tools.insert_to_cassandra_using_spark(df = data, 
                                                        passed_table_name = passed_table_name, 
                                                        passed_connection_host = passed_connection_host)     

        print(f'INFO:  Results Saved in Cassandra.')     
    
    def read_results_from_cassandra(self, passed_table_name : str, passed_connection_host: str = '10.1.10.66'):  
        data = self.cassandra_spark_tools.get_spark_data(passed_table_name, passed_connection_host = passed_connection_host)
        data = data.toPandas()

        print(f'INFO:  Results Read from Cassandra.')  
        print(f'INFO:  Process Complete.')   
        return data
    
