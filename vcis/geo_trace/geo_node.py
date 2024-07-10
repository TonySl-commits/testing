import osmnx as ox
import networkx as nx

class GeoNode():
    def __init__(self, geo_node_id, latitude, longitude, osm_id=None, node_latitude=None, node_longitude=None, node_distance=0, geo_distance=0, next_node=None, previous_node=None, path=None):
        self.geo_node_id = geo_node_id
        self.latitude = latitude
        self.longitude = longitude

        self.osm_id = osm_id
        self.node_latitude = node_latitude
        self.node_longitude = node_longitude
        
        self.node_distance = node_distance
        self.geo_distance = geo_distance

        self.path_id = path
        self.next_node = next_node
        self.previous_node = previous_node

    def find_osmnx_node(self, graph):
        node, offset = self.find_nearest_node(graph, (self.latitude, self.longitude))

        # Get the Open Street Map Node ID
        self.osm_id = node

        # Get the node coordinates
        self.node_latitude = graph.nodes[node]['y']
        self.node_longitude = graph.nodes[node]['x']
        
    def find_nearest_node(self, graph, point, return_dist=True):
        # Find the nearest network node to the given coordinates
        nearest_node, distance = ox.distance.nearest_nodes(graph, point[1], point[0], return_dist=return_dist)

        return nearest_node, distance
    
    def compute_distance_from_previous_node(self, graph, previous_node):
        if previous_node is not None:
            # Get the previous node's geospatial coordinates
            previous_geo_coordinates = (previous_node.latitude, previous_node.longitude)
            
            # Get the previous node's coordinates
            current_geo_coordinates = (self.latitude, self.longitude)
            
            # Compute the distance between the previous node's geo coordinates and the current node's geo coordinates
            geo_distance = ox.distance.great_circle(previous_geo_coordinates[0], previous_geo_coordinates[1], 
                                                    current_geo_coordinates[0], current_geo_coordinates[1])
            
            # Calculate the shortest path and its length between the nearest nodes
            previous_node_osm_id = previous_node.osm_id
            current_node_osm_id = self.osm_id
            
            node_distance = nx.shortest_path_length(graph, previous_node_osm_id, current_node_osm_id, weight='length')
        
            # Set the node distance and the geo distance
            self.node_distance = node_distance
            self.geo_distance = geo_distance
        
    def evaluate_node_addition(self):
        # Compare the geo distance and the node distance
        if self.node_distance < 1.5 * self.geo_distance:
            return True
        
        return False
    
    def get_path_to_node(self, graph):
        if self.previous_node is not None:
            # Get the shortest path from the previous node to the current node
            self.path_id = nx.shortest_path(graph, self.previous_node.osm_id, self.osm_id, weight='length')
