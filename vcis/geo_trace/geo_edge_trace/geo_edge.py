import osmnx as ox
import networkx as nx
import geopandas as gpd
from shapely.geometry import Point, LineString
from shapely.ops import nearest_points
from geopy.distance import geodesic
import folium
from soupsieve import closest

class GeoEdge():
    def __init__(self, latitude, longitude, geo_edge_id, 
                osmnx_edge_id=None, osmnx_node_id=None, 
                start_node=None, end_node=None, geo_edges=None, geo_distance=0, 
                path=None, next_node=None, previous_node=None):
        
        # Latitude and longitude of Geosptatial Point
        self.latitude = latitude
        self.longitude = longitude
        
        # Geo Edge ID 
        self.geo_edge_id = geo_edge_id
        
        # Open Street Map Edge ID
        self.osmnx_edge_id = osmnx_edge_id
        
        # Open Street Map Node ID
        self.osmnx_node_id = osmnx_node_id

        # Start and End Nodes of Geo Edge
        self.start_node = start_node
        self.end_node = end_node
        
        # Set of Nearest Edges
        self.geo_edges = geo_edges
        
        # Geo Distance of Point to Geo Edge
        self.geo_distance = geo_distance

        # Path to Geo Edge
        self.path = path
        
        # Next and Previous Geo Edge Nodes
        self.next_node = next_node
        self.previous_node = previous_node
        
    def set_edge_information(self, edge):
        # Set the edge information
        self.osmnx_edge_id = edge['osmid']
        self.start_node = edge['u']
        self.end_node = edge['v']
        self.geo_distance = edge['distance']
        
    # def get_osmnx_node(self, graph):
    #     node = self.find_nearest_node(graph, (self.latitude, self.longitude))

    #     # Get the Open Street Map Node ID
    #     self.osmnx_node_id = node
        
    # def find_nearest_node(self, graph, point, return_dist=False):
    #     # Find the nearest network node to the given coordinates
    #     nearest_node = ox.distance.nearest_nodes(graph, point[1], point[0], return_dist=return_dist)

    #     return nearest_node
    
    def find_osmnx_edge(self, graph):
        # Find nearest graph edges
        closest_edges = self.find_nearest_graph_edges(graph, (self.latitude, self.longitude))

        print('Closest Edges: \n', closest_edges)

        if closest_edges is None:
            return False

        # Set the edges set
        self.geo_edges = closest_edges
        
        # Evaluate node addition
        add_node = self.evaluate_geo_edge_node_addition(closest_edges)
        
        if add_node:
            # Select most promising edge
            closest_edge = dict(closest_edges.iloc[0])
            
            # Set the edge information
            self.set_edge_information(closest_edge)
            
        return add_node

    def find_nearest_graph_edges(self, graph, point, buffer_radius = 0.001):
        # Create a point object from your coordinates
        point_of_interest = Point(point[1], point[0])

        # Load the graph and convert to GeoDataFrame
        edges = ox.graph_to_gdfs(graph, nodes=False, edges=True)

        # Create a buffer around your point if needed (optional, for broader searches)
        buffered_point_of_interest = point_of_interest.buffer(buffer_radius)

        # Find edges within the buffer
        potential_edges = edges[edges.intersects(buffered_point_of_interest)]

        # Calculate the nearest edge explicitly
        def calculate_distance(row):
            # Retrieve the nearest point on the line to the point of interest
            line = row.geometry
            nearest_geom = nearest_points(point_of_interest, line)[1]  # This gives you the closest point on the edge

            # Use geodesic distance which calculates distance on the surface of a sphere
            distance = geodesic((point_of_interest.y, point_of_interest.x), (nearest_geom.y, nearest_geom.x)).meters
            distance = round(distance, 2)
            return distance
        
        if potential_edges.empty:
            print('No edges found within the buffer')
            return None

        potential_edges.loc[:, 'distance'] = potential_edges.apply(calculate_distance, axis=1)
        closest_edges = potential_edges.nsmallest(5, 'distance')  # Get the 5 closest edges

        closest_edges.reset_index(inplace=True)
        closest_edges.drop(columns=['key', 'bridge', 'oneway', 'lanes', 'ref', 'name', 'highway', 'reversed', 'length', 'maxspeed', 'junction', 'tunnel'], inplace=True)
        
        return closest_edges
    
    def evaluate_geo_edge_node_addition(self, closest_edges):
        # Get the average distance from the Geo Point to the nearest edges
        average_distance = closest_edges['distance'].mean().round(2)
        
        # If the average distance is greater than 10 meters, add the node to the list
        add_node = True if average_distance > 10 else False
            
        return add_node
    
    def get_path_to_node(self, graph):
        first_edge = dict(self.geo_edges.iloc[0])
        second_edge = dict(self.geo_edges.iloc[1])
        
        # If node is close to two edges, attempt shortest path from the previous start node to the start nodes of both potential edges, then see which one best leads to the next start node
        if first_edge['distance'] < 10 and second_edge['distance'] < 10:
            # Get the shortest path from the previous start node to the start nodes of both potential edges
            first_path_part_one = nx.shortest_path(graph, self.previous_node.end_node, first_edge['u'], weight='length')
            second_path_part_one = nx.shortest_path(graph, self.previous_node.end_node, second_edge['u'], weight='length')
            
            # print(first_path_part_one)
            # print(second_path_part_one)

            # # Extend both passes to the start node of the next geo edge node
            # first_path_part_two = nx.shortest_path(graph, first_edge['u'], self.next_node.start_node, weight='length')
            # second_path_part_two = nx.shortest_path(graph, second_edge['u'], self.next_node.start_node, weight='length')
            
            if self.next_node is not None:
                # Get the length of each shortest path
                first_path_part_one_length = nx.shortest_path_length(graph, self.previous_node.end_node, first_edge['u'], weight='length')
                second_path_part_one_length = nx.shortest_path_length(graph, self.previous_node.end_node, second_edge['u'], weight='length')
                
                first_path_part_two_length = nx.shortest_path_length(graph, first_edge['u'], self.next_node.start_node, weight='length')
                second_path_part_two_length = nx.shortest_path_length(graph, second_edge['u'], self.next_node.start_node, weight='length')
                
                # Get the length of both paths
                first_path_length = first_path_part_one_length + first_path_part_two_length
                second_path_length = second_path_part_one_length + second_path_part_two_length
                
                if first_path_length <= second_path_length:
                    self.path = first_path_part_one.extend([first_edge['v']])
                else:
                    # Set the new edge information
                    self.set_edge_information(second_edge)
                    
                    # Get the path involving the second edge
                    self.path = second_path_part_one.extend([second_edge['v']])
                    print("Second edge taken instead of the first!")

            else:
                self.path = first_path_part_one.extend([first_edge['v']])
            
        else:
            # Get the shortest path from the previous start node to the start nodes of both potential edges
            path = nx.shortest_path(graph, self.previous_node.end_node, first_edge['u'], weight='length')
            self.path = path.extend([first_edge['v']])

    def view_node(self):
        node_view =  f"""
            Geo Node ID: {self.geo_edge_id},
            Latitude: {self.latitude},
            Longitude: {self.longitude},
            Geo Distance: {self.geo_distance}
        \n
        """
        
        print(node_view)

