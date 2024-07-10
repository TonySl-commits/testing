import pandas as pd
import osmnx as ox
import folium

from vcis.geo_trace.geo_edge_trace.trace_tools import TraceTools
from vcis.geo_trace.geo_edge_trace.geo_stay_point import GeoStayPoint
from vcis.geo_trace.geo_edge_trace.geo_road_node import GeoRoadNode

class GeoNodeList:
    def __init__(self):
        self.head = None
        self.tail = None

        self.graph = None
        
        self.trace_result = None
        self.trace_tools = TraceTools()

    def set_graph(self, graph):
        self.graph = graph

    def append_node(self, new_node):
        if self.head is None:
            self.head = new_node
            self.tail = new_node
        else:
            self.tail.next_node = new_node
            new_node.previous_node = self.tail
            self.tail = new_node

    def get_length(self):
        length = 0
        current_node = self.head
        while current_node:
            length += 1
            current_node = current_node.next_node
        print(f"Length of NodeList: {length}")
        return length

    def create_node_list(self, data, verbose=True):
        # Get the number of rows in the data
        rows = data.shape[0] - 1
        
        print(f"Number of Rows: {rows}")
        print("Data \n", data)
        
        # Set the current row index to 0
        row_index = -1
        
        # Iterate over the rows of the data until the end
        while row_index < rows:
            
            # Increment the row index
            row_index += 1
            
            # Get the current row
            current_row = data.iloc[row_index]
            
            # Create new node
            if current_row['Label'] == 'Stay Point':
                new_node = GeoStayPoint(
                    geo_edge_id=row_index,
                    latitude=current_row['Latitude'],
                    longitude=current_row['Longitude'],
                    start_timestamp=current_row['Start_Time'],
                    end_timestamp=current_row['End_Time']
                )
                
                if verbose:
                    print(f"GeoStayPoint Node {row_index} Created.")

            else:
                new_node = GeoRoadNode(
                    geo_edge_id=row_index,
                    latitude=current_row['Latitude'],
                    longitude=current_row['Longitude'],
                    timestamp=current_row['Start_Time']
                )
                
                if verbose:
                    print(f"GeoRoadNode Node {row_index} Created.")
                    
            # Add node to the list, if conditions are satisfied
            add_node = new_node.find_osmnx_edge(self.graph)
            
            # At the first iteration, add head node unconditionally
            if row_index == 0:
                self.append_node(new_node)
                
                # Skip the rest of the first iteration
                continue

            if verbose:
                new_node.view_node()
            
            # Add Node if condition is true. Otherwise, disregard the node
            if add_node:
                # Append the node to the list
                self.append_node(new_node)
                
                if verbose: 
                    print(f"\nNode {row_index} Added to NodeList: {new_node.geo_edge_id}\n")
                
            else:
                
                if verbose:
                    print(f"\nNode {row_index} was not inserted into NodeList: {new_node.geo_edge_id}\n")
                    
        # Trace path
        print('INFO:    Tracing Path in GeoNodeList Started!\n')
        self.trace_path()
                
        return self
    
    def trace_path(self, verbose = False):
        current_node = self.head
        
        # Set the path to each node
        while current_node.next_node is not None:

            # Start by skipping the first node
            current_node = current_node.next_node

            current_node.get_path_to_node(self.graph)
            
    def view_node_list(self):
        current_node = self.head
        
        while current_node:
            current_node.view_node()
            current_node = current_node.next_node
            
    def get_trace_result(self):
        trace_result = []
        current_node = self.head
        
        iterationNumber = 0
        print("Iteration Number: ", iterationNumber)
        
        # Check the type of the first node in the path
        if isinstance(current_node, GeoStayPoint):
            start_timestamp = current_node.start_timestamp
            end_timestamp = current_node.end_timestamp
            node_type = 'Stay Point'
            
        elif isinstance(current_node, GeoRoadNode):
            start_timestamp = current_node.timestamp
            end_timestamp = current_node.timestamp
            node_type = 'Road Node'
            
        # Add the first node in the path
        node_data = {
            'StartTime': start_timestamp,
            'EndTime': end_timestamp,
            'Latitude': current_node.latitude,
            'Longitude': current_node.longitude,
            'OSM_ID': current_node.start_node,
            'NodeLatitude': None,
            'NodeLongitude': None,
            'NodeType': node_type
        }
        
        previous_timestamp = end_timestamp
        
        # Add the node to the trace result
        trace_result.append(node_data)

        # Iterate over the nodes
        while current_node.next_node is not None:

            # Move to the next node
            current_node = current_node.next_node

            # If the node is a GeoStayPoint
            if isinstance(current_node, GeoStayPoint):
                
                iterationNumber += 1
                print("Iteration Number: ", iterationNumber)
                
                # Unroll the path
                path = current_node.path
                
                # Create timestamp series
                timestamps = pd.date_range(start=previous_timestamp, end=current_node.start_timestamp, periods=len(path))
                
                # Iterate over the intermediate nodes in the path
                for i, path_node_id in enumerate(path[:-1]):
                    node_data = {
                        'StartTime': timestamps[i + 1],
                        'EndTime': timestamps[i + 1],
                        'Latitude': None,
                        'Longitude': None,
                        'OSM_ID': path_node_id,
                        'NodeLatitude': None,
                        'NodeLongitude': None,
                        'NodeType': 'Intermediate'
                    }
                    trace_result.append(node_data)
                
                # Add the last node in the path 
                node_data = {
                        'StartTime': current_node.start_timestamp,
                        'EndTime': current_node.end_timestamp,
                        'Latitude': current_node.latitude,
                        'Longitude': current_node.longitude,
                        'OSM_ID': current_node.end_node,
                        'NodeLatitude': None,
                        'NodeLongitude': None,
                        'NodeType': 'Stay Point'
                    }
                
                previous_timestamp = current_node.end_timestamp
                trace_result.append(node_data)
            
            # If the node is a GeoRoadNode
            elif isinstance(current_node, GeoRoadNode):
                # Unroll the path
                path = current_node.path
                
                # Create timestamp series
                timestamps = pd.date_range(start=previous_timestamp, end=current_node.timestamp, periods=len(path))
                
                # Iterate over the intermediate nodes in the path
                for i, path_node_id in enumerate(path[:-1]):
                    node_data = {
                        'StartTime': timestamps[i + 1],
                        'EndTime': timestamps[i + 1],
                        'Latitude': None,
                        'Longitude': None,
                        'OSM_ID': path_node_id,
                        'NodeLatitude': None,
                        'NodeLongitude': None,
                        'NodeType': 'Intermediate'
                    }
                    trace_result.append(node_data)
                    
                # Add the last node in the path 
                node_data = {
                        'StartTime': current_node.timestamp,
                        'EndTime': current_node.timestamp,
                        'Latitude': current_node.latitude,
                        'Longitude': current_node.longitude,
                        'OSM_ID': current_node.end_node,
                        'NodeLatitude': None,
                        'NodeLongitude': None,
                        'NodeType': 'Road Node'
                    }
                
                previous_timestamp = current_node.timestamp
                trace_result.append(node_data)
            
        # Check the type of the last node in the path
        if isinstance(current_node, GeoStayPoint):
            start_timestamp = current_node.start_timestamp
            end_timestamp = current_node.end_timestamp
            node_type = 'Stay Point'
            
        elif isinstance(current_node, GeoRoadNode):
            start_timestamp = current_node.timestamp
            end_timestamp = current_node.timestamp
            node_type = 'Road Node'
            
        # Add the last node in the path
        node_data = {
            'StartTime': start_timestamp,
            'EndTime': end_timestamp,
            'Latitude': current_node.latitude,
            'Longitude': current_node.longitude,
            'OSM_ID': current_node.end_node,
            'NodeLatitude': None,
            'NodeLongitude': None,
            'NodeType': node_type
        }
        
        trace_result.append(node_data)

        # Create the DataFrame
        trace_result = pd.DataFrame(trace_result)
        self.trace_result = trace_result
        print(trace_result)

        return trace_result
    
    
    def format_trace_result(self, trace_result):
        # Format datetime columns
        trace_result['StartTime'] = pd.to_datetime(trace_result['StartTime']).dt.strftime('%Y-%m-%d %H:%M:%S')
        trace_result['EndTime'] = pd.to_datetime(trace_result['EndTime']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        print(trace_result)
        
        # Get the node latitude and Longitude
        node_coords = trace_result[['OSM_ID', 'NodeLatitude', 'NodeLongitude']].drop_duplicates().reset_index(drop=True)
        # node_coords = trace_result[['OSM_ID', 'NodeLatitude', 'NodeLongitude']].reset_index(drop=True)

        non_available_node_coords = node_coords[node_coords['NodeLatitude'].isnull()].reset_index(drop=True)
        available_node_coords = node_coords[~node_coords['NodeLatitude'].isnull()].reset_index(drop=True)
        
        # Get the NodeLatitude and NodeLongitude of the Nearest Node from the graph
        for index, row in non_available_node_coords.iterrows():
            # Get the Open Street Map ID
            node = row['OSM_ID']
            
            # Get the node coordinates
            non_available_node_coords.loc[index, 'NodeLatitude'] = self.graph.nodes[node]['y']
            non_available_node_coords.loc[index, 'NodeLongitude'] = self.graph.nodes[node]['x']
            
        # Concatenate the two dataframes
        all_node_coords = pd.concat([available_node_coords, non_available_node_coords], axis=0)
        
        # Drop the NodeLatitude and NodeLongitude columns from the trace result
        trace_result.drop(columns=['NodeLatitude', 'NodeLongitude'], inplace=True)
        
        # Merge the trace result with the all_node_coords
        trace_result = pd.merge(trace_result, all_node_coords, on='OSM_ID')
        
        # Reorder the columns
        trace_result = trace_result[['StartTime', 'EndTime', 'Latitude', 'Longitude', 'OSM_ID', 'NodeLatitude', 'NodeLongitude', 'NodeType']]
        print(trace_result)
        
        return trace_result
    
    def find_street_name_and_length(self, trace_result):

        for start_idx in range(0, len(trace_result) - 1):
            end_idx = start_idx + 1
            
            start_point = trace_result.iloc[start_idx]['OSM_ID']
            end_point = trace_result.iloc[end_idx]['OSM_ID']

            edge = (start_point, end_point)
            
            streetName, streetLength = self.get_edge_data(self.graph, edge)

            trace_result.loc[start_idx, 'StreetName'] = streetName
            trace_result.loc[start_idx, 'StreetLength'] = streetLength

        return trace_result
    
    def get_edge_data(self, graph, edge):
        streetName = None
        streetLength = 0

        u, v = edge
        
        if graph.has_edge(u, v):
            edge_data = graph.get_edge_data(u, v, 0)

            # Check if the edge has a street name
            if 'name' in edge_data.keys() :
                streetName = edge_data['name']

            if 'length' in edge_data.keys() :
                streetLength = edge_data['length']

        return streetName, streetLength
    
    def finalize_output(self, trace_result):
        
        trace_result = trace_result.groupby(['NodeLatitude', 'NodeLongitude']).agg({
            'StartTime': lambda x: list(x),
            'EndTime': lambda x: list(x),
            'Latitude': lambda x: list(x),
            'Longitude': lambda x: list(x),
            'NodeType': lambda x: list(x),
            'StreetName': lambda x: next((item for item in x if item is not None), ''),
        }).reset_index()

        trace_result.to_json(data_directory + "new_traced_path.json", orient='records', force_ascii=False)
        
        return trace_result
    
    
    def validate_path(self):
        current_node = self.head
        
        all_path = []
        
        # Get the entire path
        while current_node:
            # Get the current node's path, if it exists
            current_path = current_node.path
            
            if current_path is not None:
                all_path.extend(current_path)
            else:
                all_path.extend([current_node.osm_id])
            
            current_node = current_node.next_node
            
        # Check if an edge exists between two consecutive nodes in all_path
        count = 0
        for i in range(len(all_path) - 1):
            u, v = all_path[i], all_path[i+1]
            
            if u != v and not self.graph.has_edge(u, v):
                count += 1
                print(f"Edge not found between {u} and {v}. Count {count}")

        return all_path