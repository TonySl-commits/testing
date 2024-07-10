import osmnx as ox
import networkx as nx
import folium
from shapely.geometry import Point, LineString
from shapely.ops import nearest_points

# Specify the coordinates of your point of interest

# Latitude = 33.88958218170341
# Longitude = 35.55423721703177

# Latitude = 33.860002363789306
# Longitude = 35.556276138368034

# Latitude = 33.87680194832662
# Longitude = 35.539628600794146

# Latitude = 33.87418186564064
# Longitude = 35.537983117274976

Latitude = 33.88145377822556
Longitude = 35.543638024582

point_of_interest = Point(Longitude, Latitude)

# Load the graph for the area around the given point
G = ox.graph_from_point((Latitude, Longitude), dist=600, network_type='drive', simplify=False)

# Find the nearest node to the specified point
nearest_node = ox.distance.nearest_nodes(G, X=Longitude, Y=Latitude)

# Get a subgraph around this node within a certain distance (in meters)
subgraph = nx.ego_graph(G, nearest_node, radius=300, undirected=False, distance='length')

# Collect all unique edges within this subgraph
edges = list(subgraph.edges(data=False))

# Create a folium map centered around the coordinates
m = folium.Map(location=[Latitude, Longitude], zoom_start=15)

# Add marker for the original coordinate
folium.Marker([Latitude, Longitude], tooltip="Query Point", icon=folium.Icon(color='red')).add_to(m)

# Calculate the distance of each edge to the point of interest and sort by this distance
edges_with_distance = []

for u, v in edges:
    # Get edge line
    line = LineString([Point(G.nodes[u]['x'], G.nodes[u]['y']), Point(G.nodes[v]['x'], G.nodes[v]['y'])])
    
    # Get the nearest point in the line to the point of interest
    nearest_point = nearest_points(point_of_interest, line)
    nearest_point_in_line = nearest_point[1]
    
    # Get the distance between the point of interest and the nearest point in the line
    distance = ox.distance.great_circle(point_of_interest.y, point_of_interest.x, nearest_point_in_line.y, nearest_point_in_line.x)
    distance = round(distance, 2)
    
    # Add the nearest point to the map
    folium.Marker([nearest_point_in_line.y, nearest_point_in_line.x], tooltip="Nearest Point", icon=folium.Icon(color='black')).add_to(m)
    
    edges_with_distance.append((u, v, distance))

sorted_edges = sorted(edges_with_distance, key=lambda x: x[2])  # Sort by distance

# Filter edges with distance < 100 meters
sorted_edges = [edge for edge in sorted_edges if edge[2] < 100]

# print the sorted edges
for u, v, distance in sorted_edges:
    print(f"\n({u}, {v}) - Distance: {distance} meters")
    
# Plot the closest few edges
for (u, v, distance) in sorted_edges:  # Change the number to display more or fewer edges
    node_u = G.nodes[u]
    node_v = G.nodes[v]
    folium.PolyLine([(node_u['y'], node_u['x']), (node_v['y'], node_v['x'])], color='blue', weight=5, tooltip=f"Distance: {distance} meters").add_to(m)

# Show the map
m.save("map2.html")