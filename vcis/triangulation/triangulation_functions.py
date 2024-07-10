import pandas as pd
import math
from shapely.geometry import Polygon
import folium
import warnings
import folium
import numpy as np

class TriangulationFunctions:
    def __init__(self, display_map=True):
        self.display_map = display_map

    #Function 0 - Cassandra
    def merge_datasets(self,df, bts):
        return df.merge(bts[['cgi_id', 'bts_cell_name']], on='cgi_id', how='left')

    def transform_dataframe(self,df):
        selected_columns = ['usage_timeframe', 'bts_cell_name', 'cgi_id', 'location_latitude', 'location_longitude',
                            'location_azimuth', 'imsi_id', 'imei_id']
        transformed_df = df[selected_columns]
        transformed_df['Unnamed: 0'] = range(len(transformed_df))
        column_order = ['Unnamed: 0'] + selected_columns
        transformed_df = transformed_df[column_order]
        return transformed_df

#####################################################################################################################################################################################################
    #Function 1
    def extract_and_return_unique_dates_with_day(self,df):
        df['usage_timeframe'] = pd.to_datetime(df['usage_timeframe'], unit='ms')
        unique_dates = df['usage_timeframe'].dt.date.unique()
        result_list = []
        for date in unique_dates:
            day_of_week = pd.to_datetime(date).day_name()
            result_list.append(f"{date} : {day_of_week}")
        return result_list

    ##########################################################################################################################################################
    #Function 2
    # def preprocess_data(self,df):
    #     df = pd.read_csv(df)
    #     df = df.drop(columns=['Unnamed: 0'])
    #     df['usage_timeframe'] = pd.to_datetime(df['usage_timeframe'], unit='ms')
    #     columns_order = list(df.columns)
    #     columns_order.remove('location_azimuth')
    #     columns_order.insert(columns_order.index('bts_cell_name') + 1, 'location_azimuth')
    #     df = df[columns_order]
    #     return df

    def preprocess_data(self, df):
        df = df.drop(columns=['Unnamed: 0'])
        df['usage_timeframe'] = pd.to_datetime(df['usage_timeframe'], unit='ms')
        columns_order = list(df.columns)
        columns_order.remove('location_azimuth')
        columns_order.insert(columns_order.index('bts_cell_name') + 1, 'location_azimuth')
        df = df[columns_order]
        return df

    def filter_data_on_date(self,df):
        input_date = input("Enter the date (YYYY-MM-DD): ")
        try:
            input_date = pd.to_datetime(input_date).date()
            df_filtered = df[df['usage_timeframe'].dt.date == input_date]
            return df_filtered, input_date
        except ValueError:
            print("Invalid date format. Please enter the date in the format YYYY-MM-DD.")   

    def preprocess_and_filter_data(self,df):
        preprocessed_data = self.preprocess_data(df)
        processed_data, input_date = self.filter_data_on_date(preprocessed_data)
        return processed_data, input_date

    def get_user_thresholds(self):
            try:
                distance_threshold = float(input("Enter the distance threshold (in kilometers): "))
                time_threshold = float(input("Enter the time threshold (in minutes): "))
                return distance_threshold, time_threshold
            except ValueError:
                print("Invalid input. Please enter valid numeric values.")
                return self.get_user_thresholds()

    def get_filtered_data(self,processed_data, input_date):
        distance_threshold, time_threshold = self.get_user_thresholds()
        processed_data_filtered = processed_data[processed_data['usage_timeframe'].dt.date == input_date]
        return processed_data_filtered, distance_threshold, time_threshold

    ##########################################################################################################################################################
    #Function 3   

    def haversine(self,lat1, lon1, lat2, lon2):
        R = 6371  # Earth radius in kilometers
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = R * c
        return distance
        
    def possible_triangulation(self,df, distance_threshold, time_threshold):
        triangulation_rows = []
        for i in range(1, len(df)):
            if df['bts_cell_name'].iloc[i] != df['bts_cell_name'].iloc[i - 1]:
                # Use 'self.' to refer to the haversine method of the class
                distance = self.haversine(df['location_latitude'].iloc[i-1], df['location_longitude'].iloc[i-1],
                                            df['location_latitude'].iloc[i], df['location_longitude'].iloc[i])
                timeframe = (df['usage_timeframe'].iloc[i] - df['usage_timeframe'].iloc[i - 1]).total_seconds() / 60
                if distance <= distance_threshold and timeframe <= time_threshold:
                    triangulation_rows.append(df.iloc[i - 1])
                    triangulation_rows.append(df.iloc[i])
        data = pd.DataFrame(triangulation_rows)
        return data

    def compute_triangulation_data(self,processed_data_filtered, distance_threshold, time_threshold):
        data = self.possible_triangulation(processed_data_filtered, distance_threshold, time_threshold)
        data['possible_triangulation'] = [i // 2 + 1 for i in range(len(data))]
        data = data[['possible_triangulation'] + [col for col in data.columns if col != 'possible_triangulation']]
        return data

    ##########################################################################################################################################################
    #Function 4 - Parallel
    def check_common_area(self,record1, record2, distance_threshold=1.0, azimuth_threshold=10, coverage_radius=0.5):
        try:
            lat1, lon1, azimuth1 = float(record1['location_latitude']), float(record1['location_longitude']), float(record1['location_azimuth'])
            lat2, lon2, azimuth2 = float(record2['location_latitude']), float(record2['location_longitude']), float(record2['location_azimuth'])
            distance = self.calculate_distance(lat1, lon1, lat2, lon2)
            azimuth_diff = abs(azimuth1 - azimuth2)
            intersect = self.check_intersection_area(lat1, lon1, lat2, lon2, coverage_radius)
            return distance <= distance_threshold and azimuth_diff <= azimuth_threshold and intersect
        except ValueError:
            return False
        
    def calculate_distance(self,lat1, lon1, lat2, lon2):
        R = 6371  # Earth radius in kilometers
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = R * c
        return distance

    def check_intersection_area(self,lat1, lon1, lat2, lon2, coverage_radius):
        distance = self.calculate_distance(lat1, lon1, lat2, lon2)
        return distance <= 2 * coverage_radius

    def calculate_counts_and_true_df(self,data):
        true_count = 0
        false_count = 0
        unique_triangulations = data['possible_triangulation'].unique()
        true_records = []
        for triangulation in unique_triangulations:
            subset = data[data['possible_triangulation'] == triangulation]
            for i in range(len(subset)):
                for j in range(i + 1, len(subset)):
                    result = self.check_common_area(subset.iloc[i], subset.iloc[j])
                    if result:
                        true_count += 1
                        true_records.append(subset.iloc[i])
                        true_records.append(subset.iloc[j])
                    else:
                        false_count += 1
        true_df = pd.DataFrame(true_records)
        return true_count, false_count, true_df

    def get_true_df_and_counts(self,data):
        true_count, false_count, true_df = self.calculate_counts_and_true_df(data)
        true_df['usage_timeframe'] = pd.to_datetime(true_df['usage_timeframe'])
        true_df = true_df.sort_values(by='possible_triangulation')
        return true_df, true_count, false_count

    def filter_and_get_unique_locations(self,true_df):
        rows_to_keep = []
        for i in range(0, len(true_df) - 1, 2):
            row1 = true_df.iloc[i]
            row2 = true_df.iloc[i + 1]
            if (row1['location_latitude'] != row2['location_latitude']) or (row1['location_longitude'] != row2['location_longitude']):
                rows_to_keep.extend([i, i + 1])
        par_filtered_df = true_df.iloc[rows_to_keep].reset_index(drop=True)
        unique_locations_count = len(par_filtered_df[['location_latitude', 'location_longitude']].drop_duplicates())
        return par_filtered_df, unique_locations_count

    def draw_circle_around_point(self,m, latitude, longitude, radius, color='blue'):
        folium.Circle(location=[latitude, longitude], radius=radius, color=color, fill=True, fill_color=color, fill_opacity=0.1).add_to(m)

    def plot_triangle(self,m, latitude, longitude, azimuth, distance_km, color):
        calculator = self.calculate_sector_triangle(latitude, longitude, azimuth, distance_km=distance_km)
        triangle_coordinates = calculator
        triangle_polygon = Polygon(triangle_coordinates)
        folium.Polygon(locations=triangle_coordinates, color=color, fill=True, fill_color=color, fill_opacity=0.1).add_to(m)
        return triangle_polygon

    def create_map_and_plot_data_parallel(self,par_filtered_df):
        m = folium.Map(location=[par_filtered_df['location_latitude'].iloc[0], par_filtered_df['location_longitude'].iloc[0]], zoom_start=15)
        triangle_colors = ['green', 'blue', 'purple', 'orange', 'red', 'brown']
        intersection_coordinates_list = []
        unique_locations = par_filtered_df[['location_latitude', 'location_longitude']].drop_duplicates()
        for i in range(0, len(unique_locations), 2):
            latitude1, longitude1 = unique_locations.iloc[i]
            if i + 1 < len(unique_locations):
                latitude2, longitude2 = unique_locations.iloc[i + 1]
                row1 = par_filtered_df[(par_filtered_df['location_latitude'] == latitude1) & (par_filtered_df['location_longitude'] == longitude1)].iloc[0]
                row2 = par_filtered_df[(par_filtered_df['location_latitude'] == latitude2) & (par_filtered_df['location_longitude'] == longitude2)].iloc[0]
                azimuth1, popup1 = row1[['location_azimuth', 'usage_timeframe']]
                azimuth2, popup2 = row2[['location_azimuth', 'usage_timeframe']]
                triangle_polygon1 = self.plot_triangle(m, latitude1, longitude1, azimuth1, distance_km=2, color=triangle_colors[i // 2 % len(triangle_colors)])
                triangle_polygon2 = self.plot_triangle(m, latitude2, longitude2, azimuth2, distance_km=2, color=triangle_colors[i // 2 % len(triangle_colors)])
                self.draw_circle_around_point(m, latitude1, longitude1, radius=100, color='blue')
                self.draw_circle_around_point(m, latitude2, longitude2, radius=100, color='blue')
                folium.CircleMarker([latitude1, longitude1], radius=1, color='red', popup=popup1).add_to(m)
                folium.CircleMarker([latitude2, longitude2], radius=1, color='red', popup=popup2).add_to(m)
                intersection_polygon = triangle_polygon1.intersection(triangle_polygon2)
                if not intersection_polygon.is_empty:
                    intersection_coordinates = [list(coord) for coord in intersection_polygon.exterior.coords]
                    intersection_coordinates_list.append(intersection_coordinates)
                    folium.Polygon(locations=intersection_coordinates, color='yellow', fill=True, fill_color='yellow', fill_opacity=0.5).add_to(m)
                    #Print coordinates
                    print(f"Intersection Coordinates for Pair {i // 2}: {intersection_coordinates}")

        #return m
        return m, intersection_coordinates_list


    def print_intersection_coordinates(self,intersection_coordinates_list):
        flattened_list = [point for sublist in intersection_coordinates_list for point in sublist]
        unique_points = []
        for point in flattened_list:
            if point not in unique_points:
                unique_points.append(point)
        #print("\nThe unique pts in intersection_coordinates_list is:")
        print(unique_points)

    def get_min_max_dates(self,df):
        df['usage_timeframe'] = pd.to_datetime(df['usage_timeframe'])
        grouped = df.groupby('possible_triangulation')
        result_tuples = []
        for name, group in grouped:
            min_date = group['usage_timeframe'].min().strftime('%Y-%m-%d %H:%M:%S.%f')
            max_date = group['usage_timeframe'].max().strftime('%Y-%m-%d %H:%M:%S.%f')
            result_tuples.append((min_date, max_date))
        return result_tuples

    ##########################################################################################################################################################
    #Function 5- Oppose
    def filter_and_get_unique_locations_oppose(self,true_df_opp):    
        true_df_opp['usage_timeframe'] = pd.to_datetime(true_df_opp['usage_timeframe']) 
        true_df_opp = true_df_opp.sort_values(by='possible_triangulation')
        rows_to_keep_opp = []   
        for i in range(0, len(true_df_opp) - 1, 2):
            row1_opp = true_df_opp.iloc[i]
            row2_opp = true_df_opp.iloc[i + 1]
            if (row1_opp['location_latitude'] != row2_opp['location_latitude']) or (row1_opp['location_longitude'] != row2_opp['location_longitude']):
                rows_to_keep_opp.extend([i, i + 1])  
        opp_filtered_df = true_df_opp.iloc[rows_to_keep_opp].reset_index(drop=True)  
        unique_locations_count_opp = len(opp_filtered_df[['location_latitude', 'location_longitude']].drop_duplicates()) 
        return opp_filtered_df, unique_locations_count_opp

    def calculate_common_area(self,record1, record2, distance_threshold=1.0, coverage_radius=0.5):
            lat1, lon1 = float(record1['location_latitude']), float(record1['location_longitude'])
            lat2, lon2 = float(record2['location_latitude']), float(record2['location_longitude'])
            distance = self.calculate_distance(lat1, lon1, lat2, lon2)
            intersect = self.check_intersection_area(lat1, lon1, lat2, lon2, coverage_radius)
            return distance <= distance_threshold and intersect
        
    def calculate_counts_and_true_df_oppose(self,data):
            true_count = 0
            false_count = 0
            unique_triangulations = data['possible_triangulation'].unique()
            true_records = []
            for triangulation in unique_triangulations:
                subset = data[data['possible_triangulation'] == triangulation]
                for i in range(len(subset)):
                    for j in range(i + 1, len(subset)):
                        result = self.calculate_common_area(subset.iloc[i], subset.iloc[j])
                        if result:
                            true_count += 1
                            true_records.append(subset.iloc[i])
                            true_records.append(subset.iloc[j])
                        else:
                            false_count += 1
            true_df = pd.DataFrame(true_records)
            return true_df
        
    def create_map_and_plot_data_oppose(self,opp_filtered_df):
        m = folium.Map(location=[opp_filtered_df['location_latitude'].iloc[0], opp_filtered_df['location_longitude'].iloc[0]], zoom_start=15)
        triangle_colors = ['green', 'blue', 'purple', 'orange', 'red', 'brown']
        intersection_coordinates_list = []
        unique_locations = opp_filtered_df[['location_latitude', 'location_longitude']].drop_duplicates()
        for i in range(0, len(unique_locations), 2):
            latitude1, longitude1 = unique_locations.iloc[i]
            if i + 1 < len(unique_locations):
                latitude2, longitude2 = unique_locations.iloc[i + 1]     
                row1 = opp_filtered_df[(opp_filtered_df['location_latitude'] == latitude1) & (opp_filtered_df['location_longitude'] == longitude1)].iloc[0]
                row2 = opp_filtered_df[(opp_filtered_df['location_latitude'] == latitude2) & (opp_filtered_df['location_longitude'] == longitude2)].iloc[0]
                azimuth1, popup1 = row1[['location_azimuth', 'usage_timeframe']]
                azimuth2, popup2 = row2[['location_azimuth', 'usage_timeframe']]
                triangle_polygon1 = self.plot_triangle(m, latitude1, longitude1, azimuth1, distance_km=2, color=triangle_colors[i // 2 % len(triangle_colors)])
                triangle_polygon2 = self.plot_triangle(m, latitude2, longitude2, azimuth2, distance_km=2, color=triangle_colors[i // 2 % len(triangle_colors)])
                self.draw_circle_around_point(m, latitude1, longitude1, radius=100, color='blue')
                self.draw_circle_around_point(m, latitude2, longitude2, radius=100, color='blue')
                folium.CircleMarker([latitude1, longitude1], radius=1, color='red', popup=popup1).add_to(m)
                folium.CircleMarker([latitude2, longitude2], radius=1, color='red', popup=popup2).add_to(m)
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=RuntimeWarning)
                    intersection_polygon = triangle_polygon1.intersection(triangle_polygon2)
                if not intersection_polygon.is_empty:
                    intersection_coordinates = [list(coord) for coord in intersection_polygon.exterior.coords]
                    intersection_coordinates_list.append(intersection_coordinates)
                    folium.Polygon(locations=intersection_coordinates, color='yellow', fill=True, fill_color='yellow', fill_opacity=0.5).add_to(m)
                    
                    print(f"Intersection Coordinates for Pair {i // 2}: {intersection_coordinates}")
        
        return m, intersection_coordinates_list


    ##########################################################################################################################################################
    #Function 6- All maps
    def calculate_triangle_and_intersection(self,df, intersection_coordinates_list, intersection_df_yellow=pd.DataFrame(columns=['latitude', 'longitude'])):    
        m = folium.Map(location=[df['location_latitude'].iloc[0], df['location_longitude'].iloc[0]], zoom_start=15)
        previous_triangle_polygon = None
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            for i in range(len(df)):
                latitude, longitude, azimuth, popup = df.loc[i, ['location_latitude', 'location_longitude', 'location_azimuth', 'usage_timeframe']]
                azimuth = float(azimuth)
                calculator = self.calculate_sector_triangle(latitude, longitude, azimuth, distance_km=0.5)
                triangle_polygon = Polygon(calculator)
                if i > 0:
                    intersection_polygon = triangle_polygon.intersection(previous_triangle_polygon)
                    if not intersection_polygon.is_empty and intersection_polygon.geom_type == 'Polygon':
                        intersection_coordinates = [list(coord) for coord in intersection_polygon.exterior.coords]
                        intersection_coordinates_list.append(intersection_coordinates)
                        intersection_df_yellow = pd.concat([intersection_df_yellow, pd.DataFrame(intersection_coordinates, columns=['latitude', 'longitude'])], ignore_index=True)
                        folium.Polygon(locations=intersection_coordinates, color='yellow', fill=True, fill_color='yellow', fill_opacity=0.5).add_to(m)
                previous_triangle_polygon = triangle_polygon

        return m, intersection_df_yellow

    def process_intersection(self,triangle_polygon, previous_triangle_polygon, intersection_df, all_maps_m_combined):
        intersection_coordinates_list=[]
        intersection_polygon = triangle_polygon.intersection(previous_triangle_polygon)
        if not intersection_polygon.is_empty:
            if intersection_polygon.geom_type == 'Polygon':
                intersection_coordinates = [list(coord) for coord in intersection_polygon.exterior.coords]
                intersection_coordinates_list.append(intersection_coordinates)
                intersection_df = pd.concat([intersection_df, pd.DataFrame(intersection_coordinates, columns=['latitude', 'longitude'])], ignore_index=True)
                folium.Polygon(locations=intersection_coordinates, color='yellow', fill=True, fill_color='yellow', fill_opacity=0.5).add_to(all_maps_m_combined)
                for vertex in intersection_coordinates:
                    folium.Marker(vertex, popup=f"Vertex: {vertex}", icon=folium.Icon(color='blue')).add_to(all_maps_m_combined)
        return intersection_df

    def process_map_rows(self,df, distance_km, intersection_df, all_maps_m_combined):
        previous_triangle_polygon = None
        with np.errstate(invalid='ignore'):  # Suppress RuntimeWarning
            for i in range(len(df)):
                latitude, longitude, azimuth, popup = df.iloc[i][['location_latitude', 'location_longitude', 'location_azimuth', 'usage_timeframe']]
                azimuth = float(azimuth)
                calculator = self.calculate_sector_triangle(latitude, longitude, azimuth, distance_km=distance_km)
                triangle_coordinates = calculator
                triangle_polygon = Polygon(triangle_coordinates)
                if i > 0:
                    intersection_df = self.process_intersection(triangle_polygon, previous_triangle_polygon, intersection_df, all_maps_m_combined)
                previous_triangle_polygon = triangle_polygon
        return intersection_df

    def process_pairs_for_second_map(self,df, all_maps_m_combined):
        with np.errstate(invalid='ignore'):  # Suppress RuntimeWarning
            for i in range(0, len(df), 2):
                try:
                    latitude1, longitude1, azimuth1, popup1 = df.iloc[i][['location_latitude', 'location_longitude', 'location_azimuth', 'usage_timeframe']]
                    latitude2, longitude2, azimuth2, popup2 = df.iloc[i+1][['location_latitude', 'location_longitude', 'location_azimuth', 'usage_timeframe']]
                    azimuth1, azimuth2 = float(azimuth1), float(azimuth2)
                    triangle_polygon1 = Polygon(self.calculate_sector_triangle(latitude1, longitude1, azimuth1, distance_km=2))
                    triangle_polygon2 = Polygon(self.calculate_sector_triangle(latitude2, longitude2, azimuth2, distance_km=2))
                    intersection_polygon = triangle_polygon1.intersection(triangle_polygon2)
                    if not intersection_polygon.is_empty:
                        intersection_coordinates = [list(coord) for coord in intersection_polygon.exterior.coords]
                        folium.Polygon(locations=intersection_coordinates, color='yellow', fill=True, fill_color='yellow', fill_opacity=0.5).add_to(all_maps_m_combined)
                        centroid = list(intersection_polygon.centroid.coords)[0]
                        folium.Marker(centroid, popup=f"Centroid: {centroid}", icon=folium.Icon(color='green')).add_to(all_maps_m_combined)
                        for vertex in intersection_coordinates:
                            folium.Marker(vertex, popup=f"Vertex: {vertex}", icon=folium.Icon(color='blue')).add_to(all_maps_m_combined)
                except Exception as e:
                    print(f"Error processing pair {i}/{i+1}: {e}")
                    # Create an empty list and DataFrame to store intersection coordinates
                    
    ##########################################################################################################################################################                
    #Function 7
    def filter_bts_triangulation_records(self,processed_data, par_filtered_df, opp_filtered_df):
        df1 = par_filtered_df.drop(par_filtered_df.columns[0], axis=1)
        df2 = opp_filtered_df.drop(opp_filtered_df.columns[0], axis=1)
        df_update1 = pd.merge(processed_data, df1, how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
        df_update2 = pd.merge(df_update1, df2, how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
        df_without_bts_triangulation = df_update2
        return df_without_bts_triangulation 

    def create_folium_map_wo_trianagulation(self,df_without_bts_triangulation, calculate_sector_triangle):
        m = folium.Map(location=[df_without_bts_triangulation['location_latitude'].iloc[0], df_without_bts_triangulation['location_longitude'].iloc[0]], zoom_start=15)
        for i in range(len(df_without_bts_triangulation)):
            latitude = df_without_bts_triangulation['location_latitude'].iloc[i]
            longitude = df_without_bts_triangulation['location_longitude'].iloc[i]
            azimuth = df_without_bts_triangulation['location_azimuth'].iloc[i]
            popup = df_without_bts_triangulation['usage_timeframe'].iloc[i]
            distance_km = 0.15
            triangle_coordinates = calculate_sector_triangle(latitude, longitude, azimuth, distance_km)
            folium.Polygon(locations=triangle_coordinates, color='green', fill=True, fill_color='green', fill_opacity=0.1).add_to(m)
            folium.CircleMarker([latitude, longitude], radius=1, color='red', popup=popup).add_to(m)
        return m

    def calculate_sector_triangle(self,latitude, longitude, azimuth, distance_km=0.3):
            azimuth = float(azimuth)
            azimuth1 = (azimuth - 30) % 360
            azimuth2 = (azimuth + 30) % 360
            vertex1 = self.calculate_coordinates(latitude, longitude, azimuth1, distance_km)
            vertex2 = self.calculate_coordinates(latitude, longitude, azimuth2, distance_km)
            return [
                [latitude, longitude],
                list(vertex1),
                list(vertex2)]
            
    def calculate_coordinates(self,latitude, longitude, azimuth_deg, distance_km):
        earth_radius = 6371.0
        azimuth_rad = math.radians(azimuth_deg)
        new_lat = math.degrees(math.asin(math.sin(math.radians(latitude)) * math.cos(distance_km / earth_radius) +
                                    math.cos(math.radians(latitude)) * math.sin(distance_km / earth_radius) *
                                    math.cos(azimuth_rad)))
        new_lon = longitude + math.degrees(math.atan2(math.sin(azimuth_rad) * math.sin(distance_km / earth_radius) * math.cos(math.radians(latitude)),
                                                math.cos(distance_km / earth_radius) - math.sin(math.radians(latitude)) * math.sin(math.radians(new_lat))))
        return new_lat, new_lon
        
    ##########################################################################################################################################################  
    #Function 8
    def get_missing_bts_cells(self,df_initial, df_without_bts_triangulation):
        missing_bts_cells = set(df_initial['bts_cell_name']) - set(df_without_bts_triangulation['bts_cell_name'])
        return list(missing_bts_cells)

    def create_folium_map_missing_bts(self,df_initial, missing_bts_cells, calculate_sector_triangle):
        missing_bts_df = df_initial[df_initial['bts_cell_name'].isin(missing_bts_cells)]
        m = folium.Map(location=[df_initial['location_latitude'].iloc[0], df_initial['location_longitude'].iloc[0]], zoom_start=15)
        for index, row in missing_bts_df.iterrows():
            latitude = row['location_latitude']
            longitude = row['location_longitude']
            azimuth = row['location_azimuth']
            popup = row['usage_timeframe']
            triangle_coordinates = calculate_sector_triangle(latitude, longitude, azimuth)
            folium.Polygon(locations=triangle_coordinates, color='red', fill=True, fill_color='red', fill_opacity=0.1).add_to(m)
            folium.CircleMarker([latitude, longitude], radius=1, color='red', popup=str(popup)).add_to(m)
        return m

    ##########################################################################################################################################################
    #Function 9

    def process_intersection_combined(self,triangle_polygon, previous_triangle_polygon, intersection_coordinates_list, intersection_df, m_combined):
        intersection_polygon = triangle_polygon.intersection(previous_triangle_polygon)
        if not intersection_polygon.is_empty and intersection_polygon.geom_type == 'Polygon':
            intersection_coordinates = [list(coord) for coord in intersection_polygon.exterior.coords]
            intersection_coordinates_list.append(intersection_coordinates)
            intersection_df = pd.concat([intersection_df, pd.DataFrame(intersection_coordinates, columns=['latitude', 'longitude'])], ignore_index=True)
            folium.Polygon(locations=intersection_coordinates, color='yellow', fill=True, fill_color='yellow', fill_opacity=0.5).add_to(m_combined)
            for vertex in intersection_coordinates:
                folium.Marker(vertex, popup=f"Vertex: {vertex}", icon=folium.Icon(color='blue')).add_to(m_combined)
        return intersection_df

    def create_folium_map_combined(self,df,df_without_bts_triangulation, calculate_sector_triangle, distance_km=0.5):
        intersection_coordinates_list = []
        intersection_df = pd.DataFrame(columns=['latitude', 'longitude'])
        m_combined = folium.Map(location=[df['location_latitude'].iloc[0], df['location_longitude'].iloc[0]], zoom_start=15)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            for i in range(len(df)):
                latitude = df['location_latitude'].iloc[i]
                longitude = df['location_longitude'].iloc[i]
                azimuth = float(df['location_azimuth'].iloc[i])
                popup = df['usage_timeframe'].iloc[i]
                calculator = calculate_sector_triangle(latitude, longitude, azimuth, distance_km=0.5)
                triangle_coordinates = calculator
                triangle_polygon = Polygon(triangle_coordinates)
                if i > 0:
                    intersection_df = self.process_intersection_combined(triangle_polygon, previous_triangle_polygon, intersection_coordinates_list, intersection_df, m_combined)
                previous_triangle_polygon = triangle_polygon
            for i in range(0, len(df), 2):
                latitude1, longitude1, azimuth1, popup1 = df.iloc[i][['location_latitude', 'location_longitude', 'location_azimuth', 'usage_timeframe']]
                latitude2, longitude2, azimuth2, popup2 = df.iloc[i + 1][['location_latitude', 'location_longitude', 'location_azimuth', 'usage_timeframe']]
                if np.isnan(pd.to_numeric(azimuth1, errors='coerce')) or np.isnan(pd.to_numeric(azimuth2, errors='coerce')):
                    print(f"Skipping pair {i}/{i + 1} due to invalid azimuth value(s).")
                    continue
                azimuth1, azimuth2 = float(azimuth1), float(azimuth2)
                #azimuth, azimuth2= float(azimuth1), float(azimuth2)
                try:
                    triangle_polygon1 = Polygon(calculate_sector_triangle(latitude1, longitude1, azimuth1, distance_km=2))
                    triangle_polygon2 = Polygon(calculate_sector_triangle(latitude2, longitude2, azimuth2, distance_km=2))
                    intersection_polygon = triangle_polygon1.intersection(triangle_polygon2)
                    if not intersection_polygon.is_empty and intersection_polygon.geom_type == 'Polygon':
                        intersection_coordinates = [list(coord) for coord in intersection_polygon.exterior.coords]
                        intersection_coordinates_list.append(intersection_coordinates)
                        intersection_df = pd.concat([intersection_df, pd.DataFrame(intersection_coordinates, columns=['latitude', 'longitude'])], ignore_index=True)
                        folium.Polygon(locations=intersection_coordinates, color='yellow', fill=True, fill_color='yellow', fill_opacity=0.5).add_to(m_combined)
                        for vertex in intersection_coordinates:
                            folium.Marker(vertex, popup=f"Vertex: {vertex}", icon=folium.Icon(color='blue')).add_to(m_combined)
                except Exception as e:
                    print(f"Error processing pair {i}/{i + 1}: {e}")

            for i in range(len(df_without_bts_triangulation)):
                latitude = df_without_bts_triangulation['location_latitude'].iloc[i]
                longitude = df_without_bts_triangulation['location_longitude'].iloc[i]
                azimuth = df_without_bts_triangulation['location_azimuth'].iloc[i]
                popup = df_without_bts_triangulation['usage_timeframe'].iloc[i]
                distance_km = 0.15
                triangle_coordinates = calculate_sector_triangle(latitude, longitude, azimuth, distance_km)
                folium.Polygon(locations=triangle_coordinates, color='green', fill=True, fill_color='green', fill_opacity=0.1).add_to(m_combined)
                folium.CircleMarker([latitude, longitude], radius=1, color='red', popup=popup).add_to(m_combined)
            for intersection_coordinates in intersection_coordinates_list:
                folium.Polygon(locations=intersection_coordinates, color='yellow', fill=True, fill_color='yellow', fill_opacity=0.5).add_to(m_combined)
                for vertex in intersection_coordinates:
                    folium.Marker(vertex, popup=f"Vertex: {vertex}", icon=folium.Icon(color='blue')).add_to(m_combined)
            return m_combined
        
        
    ##########################################################################################################################################################    
    #Function 10
    def final_create_combined_map(self,all_df, df_without_bts_triangulation, calculate_sector_triangle):
        intersection_coordinates_list = []
        intersection_df = pd.DataFrame(columns=['latitude', 'longitude'])
        m_combined = folium.Map(location=[all_df['location_latitude'].iloc[0], all_df['location_longitude'].iloc[0]], zoom_start=15)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            for i in range(len(all_df)):
                latitude = all_df['location_latitude'].iloc[i]
                longitude = all_df['location_longitude'].iloc[i]
                azimuth = float(all_df['location_azimuth'].iloc[i])
                popup = all_df['usage_timeframe'].iloc[i]
                calculator = calculate_sector_triangle(latitude, longitude, azimuth, distance_km=0.5)
                triangle_coordinates = calculator
                triangle_polygon = Polygon(triangle_coordinates)
                if i > 0:
                    intersection_df = self.final_process_intersection(triangle_polygon, previous_triangle_polygon, intersection_coordinates_list, intersection_df, m_combined)
                previous_triangle_polygon = triangle_polygon
            for i in range(0, len(all_df), 2):
                latitude1, longitude1, azimuth1, popup1 = all_df.iloc[i][['location_latitude', 'location_longitude', 'location_azimuth', 'usage_timeframe']]
                latitude2, longitude2, azimuth2, popup2 = all_df.iloc[i + 1][['location_latitude', 'location_longitude', 'location_azimuth', 'usage_timeframe']]
                if np.isnan(pd.to_numeric(azimuth1, errors='coerce')) or np.isnan(pd.to_numeric(azimuth2, errors='coerce')):
                    print(f"Skipping pair {i}/{i + 1} due to invalid azimuth value(s).")
                    continue
                azimuth1, azimuth2 = float(azimuth1), float(azimuth2)
                try:
                    triangle_polygon1 = Polygon(calculate_sector_triangle(latitude1, longitude1, azimuth1, distance_km=2))
                    triangle_polygon2 = Polygon(calculate_sector_triangle(latitude2, longitude2, azimuth2, distance_km=2))
                    intersection_polygon = triangle_polygon1.intersection(triangle_polygon2)
                    if not intersection_polygon.is_empty and intersection_polygon.geom_type == 'Polygon':
                        intersection_coordinates = [list(coord) for coord in intersection_polygon.exterior.coords]
                        intersection_coordinates_list.append(intersection_coordinates)
                        intersection_df = pd.concat([intersection_df, pd.DataFrame(intersection_coordinates, columns=['latitude', 'longitude'])], ignore_index=True)
                        folium.Polygon(locations=intersection_coordinates, color='yellow', fill=True, fill_color='yellow', fill_opacity=0.5).add_to(m_combined)
                except Exception as e:
                    print(f"Error processing pair {i}/{i + 1}: {e}")
            for i in range(len(df_without_bts_triangulation)):
                latitude = df_without_bts_triangulation['location_latitude'].iloc[i]
                longitude = df_without_bts_triangulation['location_longitude'].iloc[i]
                azimuth = df_without_bts_triangulation['location_azimuth'].iloc[i]
                popup = df_without_bts_triangulation['usage_timeframe'].iloc[i]
                distance_km = 0.15
                triangle_coordinates = calculate_sector_triangle(latitude, longitude, azimuth, distance_km)
                folium.Polygon(locations=triangle_coordinates, color='green', fill=True, fill_color='green', fill_opacity=0.1).add_to(m_combined)
                folium.CircleMarker([latitude, longitude], radius=1, color='red', popup=popup).add_to(m_combined)
            for intersection_coordinates in intersection_coordinates_list:
                folium.Polygon(locations=intersection_coordinates, color='yellow', fill=True, fill_color='yellow', fill_opacity=0.5).add_to(m_combined)
            return m_combined

    def final_process_intersection(self,triangle_polygon, previous_triangle_polygon, intersection_coordinates_list, intersection_df, m_combined):
        intersection_polygon = triangle_polygon.intersection(previous_triangle_polygon)
        if not intersection_polygon.is_empty and intersection_polygon.geom_type == 'Polygon':
            intersection_coordinates = [list(coord) for coord in intersection_polygon.exterior.coords]
            intersection_coordinates_list.append(intersection_coordinates)
            intersection_df = pd.concat([intersection_df, pd.DataFrame(intersection_coordinates, columns=['latitude', 'longitude'])], ignore_index=True)
            folium.Polygon(locations=intersection_coordinates, color='yellow', fill=True, fill_color='yellow', fill_opacity=0.5).add_to(m_combined)
        return intersection_df









