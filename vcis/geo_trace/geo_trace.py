import osmnx as ox
import pandas as pd
import numpy as np
import math

import seaborn as sns
import matplotlib.pyplot as plt
from st_dbscan import ST_DBSCAN

from vcis.geo_trace.trace_tools import TraceTools
# from vcis.geo_trace.geo_edge_trace.geo_node_list import GeoNodeList
from vcis.geo_trace.geo_node_list import GeoNodeList


class GeoTrace:
    def __init__(self):
        self.trace_tools = TraceTools()
    
    def process_data(self, data):
        # Rename columns
        data.rename(columns={
                'location_latitude': 'Latitude', 
                'location_longitude': 'Longitude', 
                'usage_timeframe': 'Timestamp'
            }, inplace=True)
        
        # Convert timestamp column to datetime if it's not already
        data['Timestamp'] = pd.to_datetime(data['Timestamp'], unit='ms')
        data.sort_values(by='Timestamp', inplace=True)
        data.reset_index(drop=True, inplace=True)

        # Remove unnecessary columns
        data.drop(columns=['location_name', 'service_provider_id'], inplace=True)

        return data
    
    def detect_stay_points(self, data, verbose=False):      
        # Reset timestamp to seconds
        data['Timestamp'] = data['Timestamp'].apply(lambda x: x.timestamp())

        # Drop duplicated data
        data = data[['Timestamp', 'Latitude', 'Longitude']].drop_duplicates()

        # Perform Spatio-Temporal DBSCAN to detect stay points
        st_dbscan = ST_DBSCAN(eps1 = 0.05 / 6371, eps2 = 15 * 60, min_samples = 2)
        st_dbscan = st_dbscan.fit(data)

        # Get the Cluster labels
        data['Cluster'] = st_dbscan.labels

        # Drop outlier points and leave stay points only
        filtered_data = data[data['Cluster'] == -1]
        stay_points = data[data['Cluster'] != -1]

        # Reset index
        filtered_data.reset_index(inplace=True, drop=True)
        stay_points.reset_index(inplace=True, drop=True)

        # Determine the first and last timestamp for each stay point, as well as the total duration in minutes and the total hits per stay point
        stay_point_duration = stay_points.groupby('Cluster')['Timestamp'].agg([('Start_Time', 'first'), ('End_Time', 'last'), ('Size', 'count')])
        stay_point_duration['Duration'] = stay_point_duration['End_Time'] - stay_point_duration['Start_Time']
        stay_point_duration['Duration'] = (stay_point_duration['Duration'] / 60).astype(int)
        
        # Reset start and end time to datetime
        stay_point_duration['Start_Time'] = pd.to_datetime(stay_point_duration['Start_Time'], unit='s')
        stay_point_duration['End_Time'] = pd.to_datetime(stay_point_duration['End_Time'], unit='s')
        stay_point_duration.reset_index(inplace=True)

        # Determine the stay point centroid
        stay_point_location = stay_points.groupby('Cluster')[['Latitude', 'Longitude']].mean()
        stay_point_location.reset_index(inplace=True)

        # Merge two results to get stay point location and duration
        stay_points = stay_point_duration.merge(stay_point_location, left_on='Cluster', right_on='Cluster')

        # Sort the DataFrame by Time in descending order
        stay_points = stay_points.sort_values(by=['Start_Time', 'End_Time', 'Duration', 'Size'], ascending=[True, True, False, False])

        # Re-order stay_points columns
        stay_points = stay_points[['Cluster', 'Start_Time', 'End_Time', 'Latitude', 'Longitude', 'Duration', 'Size']]
        
        # Convert the 'Timestamp' column to datetime and assign to both 'Start_Time' and 'End_Time'
        filtered_data["Start_Time"] = pd.to_datetime(filtered_data['Timestamp'].astype(int), unit='s')
        filtered_data["End_Time"] = pd.to_datetime(filtered_data['Timestamp'].astype(int), unit='s')

        filtered_data["Size"] = 1
        filtered_data["Duration"] = 0
        filtered_data = filtered_data[['Cluster', 'Start_Time', 'End_Time', 'Latitude', 'Longitude', 'Duration', 'Size']]
        
        # Create Label column in stay_points
        stay_points['Label'] = 'Stay Point'

        # Create Label column in filtered_data
        filtered_data['Label'] = 'Fill'

        if verbose:
            print("INFO:    detect_stay_points Function Completed!\n\n\n")
        
        # Merge data back
        data = pd.concat([stay_points, filtered_data], axis=0, ignore_index=True)  

        # Ensure the dataframe is sorted by time
        data = data.sort_values(by='End_Time').reset_index(drop=True)

        return data
    
    def merge_stay_points(self, data):
        
        #######################################
        # Step 1: Merge consecutive stay points
        #######################################

        # Convert 'Start_Time' and 'End_Time' to datetime
        data['Start_Time'] = pd.to_datetime(data['Start_Time'])
        data['End_Time'] = pd.to_datetime(data['End_Time'])

        # Flag rows as 'stay_point' for easy filtering
        data['is_stay_point'] = data['Label'] == 'Stay Point'

        # Group by changes in 'is_stay_point' to identify consecutive blocks
        data['group'] = (data['is_stay_point'] != data['is_stay_point'].shift()).cumsum()

        # Aggregate data for 'Stay Point' groups
        agg_dict = {
            'Start_Time': 'min',
            'End_Time': 'max',
            'Latitude': 'mean',
            'Longitude': 'mean',
            'Size': 'sum'
        }

        # Only apply complex aggregation to groups labeled as 'Stay Point'
        data_merged = data[data['is_stay_point']].groupby('group').agg(agg_dict).reset_index(drop=True)
        data_merged['Label'] = 'Stay Point'  # All these entries are Stay Points

        # Calculate Duration from the merged times
        data_merged['Duration'] = (data_merged['End_Time'] - data_merged['Start_Time']).dt.total_seconds() / 60  # duration in minutes

        # Append non-'Stay Point' rows as they are
        data_non_stay = data[~data['is_stay_point']].drop(columns=['is_stay_point', 'group'])

        data = pd.concat([data_merged, data_non_stay]).sort_values(by='End_Time').reset_index(drop=True)

        ###########################################################################
        # Step 2: Merge consecutive and close fill points and stay points if needed
        ###########################################################################

        stable = False

        while not stable:
            stable = True
            stay_indices = data[data['Label'] == 'Stay Point'].index.tolist()
            remove_indices = []

            # Check each segment between stay points
            for i in range(len(stay_indices)-1):
                start = stay_indices[i]
                next_stay = stay_indices[i + 1]
                
                # Check distance between stay points
                if self.trace_tools.haversine_distance(data.loc[start, 'Longitude'], data.loc[start, 'Latitude'], data.loc[next_stay, 'Longitude'], data.loc[next_stay, 'Latitude']) < 50:
                    remove_indices.append(next_stay)
                    stable = False  # Indicate a change was made

                # Check each fill point in the segment
                for j in range(start + 1, next_stay):
                    if data.loc[j, 'Label'] == 'Fill':
                        distance = self.trace_tools.haversine_distance(data.loc[start, 'Longitude'], data.loc[start, 'Latitude'], data.loc[j, 'Longitude'], data.loc[j, 'Latitude'])
                        if distance < 50:
                            remove_indices.append(j)

            # Removing the points that are too close
            if remove_indices:
                data = data.drop(remove_indices).reset_index(drop=True)
        
        return data
    
    def evaluate_gap_in_time(self, data):
        # Get the time interval between consecutive records
        data['TimeInterval'] = data['End_Time'] - data['End_Time'].shift(1)
        data['TimeInterval'] = data['TimeInterval'].apply(lambda x: x.total_seconds()) // 60
        data['TimeInterval'] = data['TimeInterval'].fillna(0)        
        data['TimeInterval'] -= data['Duration']
        
        # Set TimeInterval to 0 where it is less than 0
        data['TimeInterval'] = data['TimeInterval'].apply(lambda x: 0 if x < 0 else x)
        
        # Seperate data between stay points and fill points
        stay_points = data[data['Cluster'] != -1]
        fill_points = data[data['Cluster'] == -1]
        
        # print number of fill points
        print("\nINFO:       Number of fill points before filtering based on time: ", len(fill_points))
        
        # Compute time between consecutive points
        fill_points['Time'] = fill_points['End_Time']- fill_points['End_Time'].shift(1) 
        fill_points['Time'] = fill_points['Time'].apply(lambda x: x.total_seconds())
        fill_points['Time'] = fill_points['Time'].fillna(0)
        
        # Get the cumulative time overall
        fill_points['Cumulative_Time'] = fill_points['Time'].cumsum()
        
        # Bin fill points every 30 seconds
        fill_points['Time_Bin'] = fill_points['Cumulative_Time'] // 20 
        
        # Filter data, leaving 1 row from every Time_Bin
        fill_points.drop_duplicates(subset=['Time_Bin'], inplace=True)
        
        # print number of fill points
        print("INFO:       Number of fill points after filtering based on time: ", len(fill_points))
        
        # Compute haversine distance between consecutive points
        fill_points['Distance'] = self.trace_tools.haversine_distance(fill_points['Latitude'], fill_points['Longitude'],
                                                                fill_points['Latitude'].shift(1), fill_points['Longitude'].shift(1))
        
        # Set Distance travelled in meters
        fill_points['Distance'] = fill_points['Distance'].fillna(0)
        fill_points['Distance'] *= 1000
        
        # Get the cumulative distance travelled overall between fill points
        fill_points['Cumulative_Distance'] = fill_points['Distance'].cumsum()
        
        # Bin distance travelled every 15 meters
        fill_points['Distance_Bin'] = fill_points['Cumulative_Distance'] // 15 
        
        # Filter data, leaving 1 row from every Distance_Bin
        fill_points.drop_duplicates(subset=['Distance_Bin'], inplace=True)
        print(f"INFO:       Number of fill points after filtering based on distance: {len(fill_points)}\n")
        
        # Drop columns from fill points
        fill_points.drop(columns=['Time', 'Time_Bin', 'Distance', 'Distance_Bin', 'Cumulative_Time', 'Cumulative_Distance'], inplace=True)
        
        # Combine stay points and fill points
        data = pd.concat([stay_points, fill_points], sort=False).sort_values('End_Time').reset_index(drop=True)
        
        data.drop(columns=['TimeInterval'], inplace=True)
        
        return data
    
    def evaluate_movement_type(self, data):
        data_segments = {}
        
        unnecessary_columns = ['Duration', 'Size', 'Cluster']
        
        for col in unnecessary_columns:
            if col in data.columns:
                data.drop(columns=[col], inplace=True)
                
        data['Start_Time'] = pd.to_datetime(data['Start_Time'])
        data['End_Time'] = pd.to_datetime(data['End_Time'])
                
        # Get distance between consecutive points
        data['Distance'] = self.trace_tools.haversine_distance(data['Latitude'], data['Longitude'],
                                                            data['Latitude'].shift(-1), data['Longitude'].shift(-1))
        data['Distance'] = data['Distance'].fillna(0)
        
        # Get time interval between consecutive points
        data['TimeInterval'] = data['End_Time'] - data['End_Time'].shift(1)
        data['TimeInterval'] = data['TimeInterval'].apply(lambda x: x.total_seconds()) / 3600
        data['TimeInterval'] = data['TimeInterval'].fillna(0)
        
        # Get the speed of the device
        data['Speed'] = data['Distance'] / data['TimeInterval']
        
        # Convert infinity speed to 0
        data['Speed'] = data['Speed'].apply(lambda x: 0 if math.isinf(x) else x)
        
        print("\nINFO:      DEVICE DISTANCE AND SPEED: \n", data)
        
        # Filter speed less than 200 meters/hour to remove outliers
        data[data['Speed'] >= 0.200]['MovementType'] = 'Moving' 
        data[data['Speed'] < 0.200]['MovementType'] = 'Stationary'
        
        # Get the average speed and standard deviation of the speed of the device
        average_speed = data['Speed'].mean()
        median_speed = data['Speed'].median()
        standard_deviation_speed = data['Speed'].std()
        
        print(f"Average Speed: {average_speed}")
        print(f"Median Speed: {median_speed}")
        print(f"Standard Deviation Speed: {standard_deviation_speed}")
        
        # plot the speed of the device wrt to end time
        fig, ax = plt.subplots()
        ax.plot(data['End_Time'], data['Speed'])
        ax.set_title('Speed of the Device')
        ax.set_xlabel('Time')
        ax.set_ylabel('Speed (km/h)')
        fig.savefig("no_outliers_speed_plot_wrt_time.png", dpi=300) 
        
        # plot the distribution of the device's speed
        fig, ax = plt.subplots()
        sns.distplot(data['Speed'], ax=ax)
        ax.set_title('Distribution of Device Speed')
        ax.set_xlabel('Speed (km/h)')
        ax.set_ylabel('Frequency')
        fig.savefig("no_outliers_speed_distribution_plot.png", dpi=300)
        
        # plot the boxplot of the device's speed
        fig, ax = plt.subplots()
        ax.boxplot(data['Speed'])
        ax.set_title('Boxplot of Device Speed')
        ax.set_xlabel('Speed (km/h)')
        ax.set_ylabel('Frequency')
        fig.savefig("no_outliers_speed_boxplot.png", dpi=300)
        
        # Segment the data based on movement type, if needed
        # print(data)
        
        return data
    
    def initialize_graphs_wrt_movement_type(self, data_segments):
        new_data_segments = {}

        # Iterate over each data segment and perform trace algorithm
        for time_period, data in data_segments.items():
            
            graph = self.trace_tools.get_geo_graph(data)

            new_data_segments[time_period] = [graph, data]

            # smaller_data_segments = self.evaluate_movement_type(data)
            
            # for smaller_time_period, [graph_type, smaller_data] in smaller_data_segments.items():
                # graph = self.trace_tools.get_geo_graph(smaller_data, network_type=graph_type)
                
            # new_data_segments[smaller_time_period] = [graph, smaller_data]
            
        return new_data_segments

    def get_edge_data(self, G, best_path):
        best_path = list(zip(best_path[:-1], best_path[1:]))
        
        street_names = []
        street_length = []
        
        # Iterate over pairs of nodes in the path to access the edges that connect them
        for (u, v) in best_path:
            street_name = None
            street_length = 0
        
            if G.has_edge(u, v):
                edge_data = G.get_edge_data(u, v, 0)

                # Check if the edge has a street name
                if 'name' in edge_data.keys() :
                    street_name = edge_data['name']
    
                if 'length' in edge_data.keys() :
                    street_length = edge_data['length']
                    
            street_names.append(street_name)
            street_length.append(street_length)
        
        # Append None for the last coordinate which does not have an outgoing edge
        street_names.append(None)
        street_length.append(0)   

        return street_names, street_length
    
    def segment_geo_history(self, data, time_gap_threshold = 30):
        # Get the time interval between consecutive records
        data['TimeInterval'] = data['End_Time'] - data['End_Time'].shift(1)
        data['TimeInterval'] = data['TimeInterval'].apply(lambda x: x.total_seconds()) // 60
        data['TimeInterval'] = data['TimeInterval'].fillna(0)        
        data['TimeInterval'] -= data['Duration']
        
        # Set TimeInterval to 0 where it is less than 0
        data['TimeInterval'] = data['TimeInterval'].apply(lambda x: 0 if x < 0 else x)
                
        # Identify large gaps in time and segment data accordingly
        gap_indices = data.index[data['TimeInterval'] > time_gap_threshold]
        start_indices = [0] + gap_indices.tolist()  # Start indices of segments
        end_indices = gap_indices.tolist()
        end_indices = [x - 1 for x in end_indices]  # End indices of segments
        
        # Adjust the last end index
        if len(end_indices) == 0 or end_indices[-1] != len(data) - 1:
            end_indices.append(len(data) - 1)
        
        # Create segments using numpy indexing
        segments = [data.iloc[start:end + 1] for start, end in zip(start_indices, end_indices)]

        # Initialize a dictionary to store the data segments
        data_segments = {}
        
        # Get the time interval for each data segment
        for segment in segments:

            if len(segment) < 3:
                print("Segment has less than 3 records. Skipping.")
                continue
            
            # Get the start and end times of the current data segment
            start_time = segment['Start_Time'].iloc[0]
            end_time = segment['End_Time'].iloc[-1]
            
            # Get the time period between the start and end times as a string
            time_period = f"{start_time}-{end_time}"
            time_period = time_period.replace(" ", "--").replace(":", "--")
            
            # Save the data segment for the current time period
            data_segments[time_period] = segment
        
        return data_segments
    
    def merge_trace_results(self, device_trace):
        for device_id, traces in device_trace.items():
            keys = sorted(traces.keys())

            for i in range(len(keys) - 1):
                current_key = keys[i]
                next_key = keys[i + 1]
                current_trace = traces[current_key]
                next_trace = traces[next_key]

                # Calculate the distance between the last point of the current trace and the first point of the next trace
                if not current_trace['Latitude'].isna().all() and not current_trace['Longitude'].isna().all():
                    last_point_current = current_trace.dropna(subset=['Latitude', 'Longitude']).iloc[-1]

                if not next_trace['Latitude'].isna().all() and not next_trace['Longitude'].isna().all():
                    first_point_next = next_trace.dropna(subset=['Latitude', 'Longitude']).iloc[0]

                distance = self.trace_tools.haversine_distance(
                    last_point_current['Latitude'], last_point_current['Longitude'],
                    first_point_next['Latitude'], first_point_next['Longitude']
                )

                if distance < 100:
                    # Mark a small gap in the new column
                    current_trace.loc[current_trace.index[-1], 'Gap'] = 1
                    next_trace.loc[next_trace.index[0], 'Gap'] = 1

                    # Fill in the missing points
                    current_trace['Gap'] = current_trace['Gap'].fillna(0)
                    next_trace['Gap'] = next_trace['Gap'].fillna(0)
                else:
                    # Mark a large gap and calculate trajectory
                    current_trace.loc[current_trace.index[-1], 'Gap'] = 2
                    next_trace.loc[next_trace.index[0], 'Gap'] = 2

                    # Fill in the missing points
                    current_trace['Gap'] = current_trace['Gap'].fillna(0)
                    next_trace['Gap'] = next_trace['Gap'].fillna(0)
                
                    # Calculate the trajectory between the two points
                    point1 = (last_point_current['Latitude'], last_point_current['Longitude'])
                    point2 = (first_point_next['Latitude'], first_point_next['Longitude'])

                    trajectory_df = self.calculate_trajectory_between_points(point1, point2)

                    # Concatenate the trajectory data in the correct order
                    combined_df = pd.concat([current_trace, trajectory_df])

                    traces[current_key] = combined_df

        return device_trace       
    
    def preprocess_data_for_christophers_trace(self, data, graph):
        
        data.reset_index(inplace=True, drop=True)

        # Get the nearest node to each geospatial point from the graph
        lat_col_index = data.columns.get_loc('Latitude')
        lon_col_index = data.columns.get_loc('Longitude')
        data['PreviousNode'] = data.apply(lambda row: self.trace_tools.find_nearest_graph_node(graph, (row.iloc[lat_col_index], row.iloc[lon_col_index])), axis=1)

        # Get the NodeLatitude and NodeLongitude of the Nearest Node from the graph
        for index, row in data.iterrows():
            node = row['PreviousNode']
            data.loc[index, 'NodeLatitude'] = graph.nodes[node]['y']
            data.loc[index, 'NodeLongitude'] = graph.nodes[node]['x']

        first_node_lat = data.iloc[0]['Latitude']
        first_node_lon = data.iloc[0]['Longitude']
        last_node_lat = data.iloc[-1]['Latitude']
        last_node_lon = data.iloc[-1]['Longitude']

        # data['AdjustedLatitude'] = np.nan
        # data['AdjustedLongitude'] = np.nan

        data.loc[0, 'AdjustedLatitude'] = first_node_lat
        data.loc[0, 'AdjustedLongitude'] = first_node_lon
        data.loc[len(data) - 1, 'AdjustedLatitude'] = last_node_lat
        data.loc[len(data) - 1, 'AdjustedLongitude'] = last_node_lon

        # Iterate over the geospatial points
        for i in range(1, len(data) - 1):
            real_lat = data.iloc[i]['Latitude']
            real_lon = data.iloc[i]['Longitude']

            prev_node_lat = data.iloc[i - 1]['NodeLatitude']
            prev_node_lon = data.iloc[i - 1]['NodeLongitude']

            next_node_lat = data.iloc[i + 1]['NodeLatitude']
            next_node_lon = data.iloc[i + 1]['NodeLongitude']

            adjusted_lat = ((prev_node_lat + next_node_lat) + (2 * real_lat)) / 4
            adjusted_lon = ((prev_node_lon + next_node_lon) + (2 * real_lon)) / 4

            data.loc[i, 'AdjustedLatitude'] = adjusted_lat
            data.loc[i, 'AdjustedLongitude'] = adjusted_lon

        data.drop(columns=['Cluster', 'TimeInterval'], inplace=True)

        data.reset_index(inplace=True, drop=True)

        # Get the new nodes for each of the geospatial points
        adj_lat_col_index = data.columns.get_loc('AdjustedLatitude')
        adj_lon_col_index = data.columns.get_loc('AdjustedLongitude')
        data['NewNode'] = data.apply(lambda row: self.trace_tools.find_nearest_graph_node(graph, (row.iloc[adj_lat_col_index], row.iloc[adj_lon_col_index])), axis=1)

        # Get the NodeLatitude and NodeLongitude of the Nearest Node from the graph
        for index, row in data.iterrows():
            node = row['PreviousNode']
            data.loc[index, 'NewNodeLatitude'] = graph.nodes[node]['y']
            data.loc[index, 'NewNodeLongitude'] = graph.nodes[node]['x']

        # print("Final data: ", data)

        # # Get the number of matches between PreviousNode and NewNode
        # print("Number of matches: ", len(data[data['PreviousNode'] == data['NewNode']]))

        # # Get the number of mismatches between PreviousNode and NewNode
        # print("Number of mismatches: ", len(data[data['PreviousNode'] != data['NewNode']]))

        return data
    
    def christophers_trace(self, data, graph):
        print('INFO:       Started Creating Geo Node List !!!')

        # Initialize the GeoNodeList
        geo_node_list = GeoNodeList()
        
        # Set Graph
        geo_node_list.set_graph(graph)
        
        # Preprocess data for Christophers Trace
        data = self.preprocess_data_for_christophers_trace(data, graph)

        # Create Node List
        geo_node_list.create_node_list(data)

        # Validate Trace Result
        geo_node_list.validate_path()

        # Get Trace Result
        trace_result = geo_node_list.get_trace_result()

        # Format trace result
        trace_result = geo_node_list.format_trace_result(trace_result)

        # Add street name and length to trace result
        trace_result = geo_node_list.find_street_name_and_length(trace_result)

        # trace_result = geo_node_list.finalize_output(trace_result)
                
        return trace_result

        
    def trace_single_device(self, data):
        # Initialize the data trace algorithm
        device_data_trace = {}
        
        # Step 1: Detect stay points
        data = self.detect_stay_points(data)
        data = self.merge_stay_points(data)
        
        # Step 2: Evaluate gap in time
        data = self.evaluate_gap_in_time(data)

        # Step 3: Segment Geo History
        data_segments = self.segment_geo_history(data)
        
        # Step 5: Evaluate Movement Type & Initialize Graphs Per Segment
        data_segments = self.initialize_graphs_wrt_movement_type(data_segments)
        
        # Iterate over each data segment and perform trace algorithm
        for time_period, [graph, data] in data_segments.items():
            
            print(f"\nINFO:       Processing data for time period: {time_period}\n")

            # Step 6: Perform Christopher's Trace Algorithm
            trace_result = self.christophers_trace(data, graph)

            # Step 7: Plot the trajectory on a map
            self.trace_tools.validate_trace_results(trace_result, time_period)

            # Store Final Result
            device_data_trace[time_period] = trace_result

        return device_data_trace
    
    def trace(self, df_history):
        # Process data
        data = self.process_data(df_history)
        
        # Get unique devices
        unique_devices = data['device_id'].unique()
        
        # Initialize the device trace dictionary, to store the roads travelled per device
        device_trace = {}
        
        # Iterate over each device provided
        for device in unique_devices:
            
            print("\nINFO:       Tracing device: " + str(device))
            
            # Filter data for specified device only
            device_data = data[data['device_id'] == device]
            
            # Run the trace algorithm
            device_data_trace = self.trace_single_device(device_data)
            
            # Store the trace in the dictionary
            device_trace[device] = device_data_trace

        # Merge data if needed
        device_trace = self.merge_trace_results(device_trace)

        # Merge all results
        traced_paths = self.merge_all_trace_results(device_trace)
            
        return traced_paths
    
    
    
    def merge_all_trace_results(self, device_trace):
        # List to hold all path_df DataFrames
        all_path_dfs = []

        # Iterate over each device's trace data
        for device, traces in device_trace.items():

            # Iterate over each time period's path_df within a device's trace data
            for time_period, path_df in traces.items():
                
                #Add device id and time period to the path_df
                path_df['device_id'] = device
                path_df['time_period'] = time_period

                # Append the DataFrame to the list
                all_path_dfs.append(path_df)

        # Concatenate all DataFrames into one
        final_result = pd.concat(all_path_dfs, ignore_index=True)

        # Create a map with all the paths
        self.trace_tools.validate_trace_results(final_result, "FULL_PATH")

        return final_result
    
    # Function that calculates the shortest path between two points
    def calculate_trajectory_between_points(self, point1, point2):

        # Get the G graph
        graph = self.trace_tools.get_geospatial_graph(point1, point2) 

        # Find the nearest nodes to the start and end points
        start_node = self.trace_tools.find_nearest_graph_node(graph, point1)
        end_node = self.trace_tools.find_nearest_graph_node(graph, point2)
        
        # Find the shortest path between these nodes
        path = self.trace_tools.find_best_path(graph, start_node, end_node)

        # Get the data from the best path
        street_names, street_lengths = self.trace_tools.get_data_from_best_path(graph, path)
        
        # Convert the nodes in the path back to coordinates
        path_coords = self.trace_tools.get_path_coordinates(graph, path)

        # Unpack coordinates into separate lists
        node_latitudes = [coord[0] for coord in path_coords]
        node_longitudes = [coord[1] for coord in path_coords]

        # Create a DataFrame
        data = {
            'StartTime': [None] * len(path),
            'EndTime': [None] * len(path),
            'Latitude': [None] * len(path),
            'Longitude': [None] * len(path),
            'OSM_ID': path,
            'NodeLatitude': node_latitudes,
            'NodeLongitude': node_longitudes,
            'NodeType': ['Missing'] * len(path),
            'StreetName': street_names,
            'StreetLength': street_lengths
        }

        # Create the DataFrame
        final_df = pd.DataFrame(data)

        # Reorder the columns
        final_df = final_df[['StartTime', 'EndTime', 'Latitude', 'Longitude', 'OSM_ID', 'NodeLatitude', 'NodeLongitude', 'NodeType', 'StreetName', 'StreetLength']]

        return final_df
        