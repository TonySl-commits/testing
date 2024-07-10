from vcis.geo_trace.geo_node import GeoNode

class GeoRoadNode(GeoNode):
    def __init__(self, geo_node_id, latitude, longitude, timestamp, osm_id=None, node_latitude=None, node_longitude=None, node_distance=0, geo_distance=0, next_node=None, previous_node=None, path=None):
        super().__init__(geo_node_id, latitude, longitude, osm_id, node_latitude, node_longitude, node_distance, geo_distance, next_node, previous_node, path)
        self.timestamp = timestamp
        
    def view_node(self):
        node_view =  f"""
            Geo Node ID: {self.geo_node_id},
            Latitude: {self.latitude},
            Longitude: {self.longitude},
            Timestamp: {self.timestamp},
            Open Street Map ID: {self.osm_id},
            Node Latitude: {self.node_latitude},
            Node Longitude: {self.node_longitude},
            Node Distance: {self.node_distance},
            Geo Distance: {self.geo_distance},
        \n
        """
        
        print(node_view)