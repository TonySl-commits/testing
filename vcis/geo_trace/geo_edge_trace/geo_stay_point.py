from vcis.geo_trace.geo_edge_trace.geo_edge import GeoEdge

class GeoStayPoint(GeoEdge): 
    def __init__(self, latitude, longitude, geo_edge_id, start_timestamp, end_timestamp, 
                osmnx_edge_id=None, osmnx_node_id=None, 
                start_node=None, end_node=None, geo_edges=None, geo_distance=0,
                path=None, next_node=None, previous_node=None):
        super().__init__(latitude, longitude, geo_edge_id, osmnx_edge_id, osmnx_node_id, 
                start_node, end_node, geo_edges, geo_distance,
                path, next_node, previous_node)
        
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        
    def view_node(self):
        node_view =  f"""
            Geo Node ID: {self.geo_edge_id},
            Latitude: {self.latitude},
            Longitude: {self.longitude},
            Start Timestamp: {self.start_timestamp},
            End Timestamp: {self.end_timestamp},
            Start Node: {self.start_node},
            End Node: {self.end_node},
            Geo Distance: {self.geo_distance}
        \n
        """
        
        print(node_view)