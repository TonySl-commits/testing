import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import os
import math
import folium
import warnings
from vcis.databases.cassandra import cassandra_tools

# Suppress the warning
warnings.filterwarnings("ignore", message="Geometry is in a geographic CRS")

class ReverseGeocoder:
    def __init__(self, shapefile_path):
        self.world = self.load_shapefile(shapefile_path)
        self.cached_results = {}
        self.cassandra_tool = cassandra_tools.CassandraTools()
        session = self.cassandra_tool.get_cassandra_connection(server="10.1.10.110")
        table_name = "countries_iso_codes"
        iso_table  = self.cassandra_tool.get_table(table_name=table_name,session=session)

        # Join Cassandra DataFrame with world DataFrame based on country name
        self.world = pd.merge(self.world, iso_table, how='right', left_on='NAME', right_on='country_name')

    
    def load_shapefile(self, shapefile_path):
        try:
            return gpd.read_file(shapefile_path)
        except Exception as e:
            print("An error occurred while reading the shapefile:")
            print(e)
            return None

    def get_step_size(self, distance=1):
        latitude = 33.8
        one_degree_lat = 110.574
        one_degree_lat_radians = math.radians(latitude)
        one_degree_long = 111.320 * math.cos(one_degree_lat_radians)
        step_lon = (distance / (one_degree_long * 1000))
        step_lat = (distance / (one_degree_lat * 1000))
        return step_lat, step_lon

    def binning_lat(self, data, step):
        to_bin = lambda x: np.floor(x / step) * step
        data.loc[:, "latitude_grid"] = data['LOCATION_LATITUDE'].apply(to_bin)
        data.loc[:, "latitude_grid"] = data['latitude_grid'] + step / 2
        return data

    def binning_lon(self, data, step):
        to_bin = lambda x: np.floor(x / step) * step
        data.loc[:, "longitude_grid"] = data['LOCATION_LONGITUDE'].apply(to_bin)
        data.loc[:, "longitude_grid"] = data['longitude_grid'] + step / 2
        return data

    def binning(self, data, distance=200):
        step_lat, step_lon = self.get_step_size(distance=distance)
        data = self.binning_lat(data, step_lat)
        data = self.binning_lon(data, step_lon)
        return data

    def reverse_geocode_point(self, point):
        if point in self.cached_results:
            return self.cached_results[point]
        
        possible_matches_idx = list(self.world.sindex.intersection(point.bounds))
        possible_matches = self.world.iloc[possible_matches_idx]
        country_info = possible_matches[possible_matches.geometry.contains(point)][['NAME', 'country_code']].to_dict(orient='records')
        
        if not country_info:
            closest_idx = self.world['geometry'].distance(point).idxmin()
            country_info = [{'NAME': self.world.loc[closest_idx, 'NAME'], 'country_code': self.world.loc[closest_idx, 'country_code']}]
        
        self.cached_results[point] = country_info
        return country_info

    def reverse_geocode_multipoints(self, df, with_Binning=False):
        if with_Binning:
            points = [Point(xy) for xy in zip(df['longitude_grid'], df['latitude_grid'])]
        else:
            points = [Point(xy) for xy in zip(df['LOCATION_LONGITUDE'], df['LOCATION_LATITUDE'])]

        country_info = []
        for point in points:
            result = self.reverse_geocode_point(point)
            # Make sure the result dictionary has both 'NAME' and 'country_code' keys
           
            country_info.extend(result)
        return country_info


    def reverse_geocode_with_Binning(self, df, step_distance=200):
        df = self.binning(df, step_distance)
        country_names = self.reverse_geocode_multipoints(df, with_Binning=True)
        return country_names

    def generate_incomplete_circle(self, center_lat, center_lon, radius_km, num_points):
        points = []
        angle_increment = 360 / num_points
        for i in range(num_points):
            angle = math.radians(angle_increment * i)
            lon = center_lon + (radius_km / (111.32 * math.cos(math.radians(center_lat)))) * math.cos(angle)
            lat = center_lat + (radius_km / 111.32) * math.sin(angle)
            points.append((lon, lat))  # Note the reversal of lat and lon here
        return points

    def plot_circle_on_map(self, center_lat, center_lon, radius_km, num_points):
        # Create map centered at the given coordinates
        m = folium.Map(location=[center_lat, center_lon], zoom_start=2)

        # Generate points on the incomplete circle
        circle_points = self.generate_incomplete_circle(center_lon, center_lat, radius_km, num_points)
        # Convert points to Polygon object
        polygon = Polygon(circle_points)
        # Plot the polygon on the map
        folium.Polygon(locations=polygon.exterior.coords, fill_color="blue").add_to(m)

        return m

    def check_circle_borders(self, center_lat, center_lon, radius_km, num_points):
        circle = self.generate_incomplete_circle(center_lat, center_lon, radius_km, num_points)
        polygon = Polygon(circle)
        return self.check_polygon_borders(polygon)


    def check_polygon_borders(self, polygon):
        # Calculate the bounding box of the polygon
        polygon_bbox = polygon.bounds
        # Find possible matches using the spatial index
        possible_matches_idx = list(self.world.sindex.intersection(polygon_bbox))
        possible_matches = self.world.iloc[possible_matches_idx]
        # Filter countries that contain the polygon
        countries_info = possible_matches[possible_matches.geometry.intersects(polygon)][['NAME', 'country_code']].to_dict(orient='records')
        # If no countries contain the polygon, find the closest one
        if not countries_info:
            closest_idx = self.world['geometry'].distance(polygon).idxmin()
            country_info = [{'NAME': self.world.loc[closest_idx, 'NAME'], 'country_code': self.world.loc[closest_idx, 'country_code']}]
        return countries_info
    


#     # Load shapefile
# current_dir = os.path.dirname(os.path.abspath(__file__))
# data_dir = os.path.join(current_dir, 'data')
# shapefile_path = os.path.join(data_dir, 'buffered_world.shp')

# reverse_geocoder = ReverseGeocoder(shapefile_path)
# # columns = reverse_geocoder.world.columns
# # for column in columns:
# #     print(column)
# # from vcis.databases.cassandra import cassandra_tools
# # cassandra_tool = cassandra_tools.CassandraTools()
# # session = cassandra_tool.get_cassandra_connection(server="10.1.10.110")
# # table_name = "countries_iso_codes"
# # iso_table  = cassandra_tool.get_table(table_name=table_name,session=session)
# # print(iso_table.columns)


# # Test reverse geocoding for a single point
# point = Point(-110, 40)
# print("Reverse geocoding for a single point:")
# print(reverse_geocoder.reverse_geocode_point(point))

# # Test reverse geocoding for multiple points
# df = pd.DataFrame({'LOCATION_LONGITUDE': [-110, -100, -90], 'LOCATION_LATITUDE': [40, 30, 20]})
# print("\nReverse geocoding for multiple points:")
# print(reverse_geocoder.reverse_geocode_multipoints(df))

# # Test reverse geocoding with binning
# print("\nReverse geocoding with binning:")
# print(reverse_geocoder.reverse_geocode_with_Binning(df, step_distance=200))

# # Test generating an incomplete circle
# center_lat, center_lon = 40, -110
# radius_km = 1000
# num_points = 10
# print("\nGenerating an incomplete circle:")
# print(reverse_geocoder.generate_incomplete_circle(center_lat, center_lon, radius_km, num_points))


# # Test checking circle borders
# print("\nChecking circle borders:")
# print(reverse_geocoder.check_circle_borders(center_lat, center_lon, radius_km, num_points))

# # Test checking polygon borders
# polygon = Polygon(reverse_geocoder.generate_incomplete_circle(center_lat, center_lon, radius_km, num_points))
# print("\nChecking polygon borders:")
# print(reverse_geocoder.check_polygon_borders(polygon))