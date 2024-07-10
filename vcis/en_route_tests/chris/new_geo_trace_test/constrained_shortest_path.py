import osmnx as ox
import networkx as nx
import folium

def constrained_shortest_path(G, start_node, end_node, through_edge):
    """
    G: NetworkX graph
    start_node: Starting node ID
    end_node: Destination node ID
    through_edge: A tuple (u, v) representing the edge that must be included in the path
    """
    u, v = through_edge[0], through_edge[1]

    # Shortest path from start_node to u (one end of the specified edge)
    path_to_u = nx.shortest_path(G, source=start_node, target=u, weight='length')

    # Shortest path from v (other end of the specified edge) to end_node
    path_from_v = nx.shortest_path(G, source=v, target=end_node, weight='length')

    # Combine the paths, ensuring not to repeat the node v
    path = path_to_u + path_from_v

    return path


# Coordinates for start and end points
start_latitude = 33.8607379477846
start_longitude = 35.55166062152879
end_latitude = 33.86292603839971
end_longitude = 35.54847709972986

# Load the graph
G = ox.graph_from_point((start_latitude, start_longitude), dist=5000, network_type='drive', simplify=False)

# Find the nearest nodes to start and end points
start_node = ox.distance.nearest_nodes(G, X=start_longitude, Y=start_latitude)
end_node = ox.distance.nearest_nodes(G, X=end_longitude, Y=end_latitude)

# Specify the through edge (make sure this edge exists in your graph)
through_edge = [2696639920, 3047538512]

# Get the constrained shortest path
path = constrained_shortest_path(G, start_node, end_node, through_edge)

# Create a folium map centered at the midpoint of start and end points
map_center = [(start_latitude + end_latitude) / 2, (start_longitude + end_longitude) / 2]
m = folium.Map(location=map_center, zoom_start=15, tiles='cartodbpositron')

# Plot the path using polyline
path_coords = [(G.nodes[node]['y'], G.nodes[node]['x']) for node in path]
folium.PolyLine(path_coords, color='blue', weight=5).add_to(m)

# Add markers for the start and end points
folium.Marker([start_latitude, start_longitude], tooltip="Start Point", icon=folium.Icon(color='green')).add_to(m)
folium.Marker([end_latitude, end_longitude], tooltip="End Point", icon=folium.Icon(color='red')).add_to(m)

# Optionally, display start, through, and end nodes
folium.Marker([G.nodes[start_node]['y'], G.nodes[start_node]['x']], popup='Start Node', icon=folium.Icon(color='green')).add_to(m)
folium.Marker([G.nodes[end_node]['y'], G.nodes[end_node]['x']], popup='End Node', icon=folium.Icon(color='red')).add_to(m)
u, v = through_edge[0], through_edge[1]
folium.Marker([G.nodes[u]['y'], G.nodes[u]['x']], popup='Through Node Start', icon=folium.Icon(color='orange')).add_to(m)
folium.Marker([G.nodes[v]['y'], G.nodes[v]['x']], popup='Through Node End', icon=folium.Icon(color='orange')).add_to(m)

# Display the map
m.save("map3.html")