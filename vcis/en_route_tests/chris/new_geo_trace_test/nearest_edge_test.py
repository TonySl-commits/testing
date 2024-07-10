import osmnx as ox
import folium
from folium.features import DivIcon

# Specify the coordinates of the point of interest
# Latitude = 33.86181411504806
# Longitude = 35.550111291014225

# Latitude = 33.8694312067533
# Longitude = 35.54113227908647

Latitude = 33.87680194832662
Longitude = 35.539628600794146

# Load the graph for the area around the given point
G = ox.graph_from_point((Latitude, Longitude), dist=500, network_type='drive', simplify=False)

# Find the nearest edge to the specified point
nearest_edge = ox.distance.nearest_edges(G, X=Longitude, Y=Latitude)

# Extract nodes of the nearest edge
u, v, key = nearest_edge
node_u = G.nodes[u]
node_v = G.nodes[v]

# Create a folium map centered around the coordinates
m = folium.Map(location=[Latitude, Longitude], zoom_start=15, tiles='cartodbpositron')

# Add marker for the original coordinate
folium.Marker([Latitude, Longitude], tooltip="Query Point", icon=folium.Icon(color='red')).add_to(m)

# Draw the nearest edge using the locations of its nodes
folium.PolyLine([(node_u['y'], node_u['x']), (node_v['y'], node_v['x'])], color='blue', weight=5).add_to(m)

# Optionally, display node IDs on the map
folium.map.Marker(
    [node_u['y'], node_u['x']],
    icon=DivIcon(
        icon_size=(250,36),
        icon_anchor=(0,0),
        html=f'<div style="font-size: 10pt">Node {u}</div>',
        )
    ).add_to(m)

folium.map.Marker(
    [node_v['y'], node_v['x']],
    icon=DivIcon(
        icon_size=(250,36),
        icon_anchor=(0,0),
        html=f'<div style="font-size: 10pt">Node {v}</div>',
        )
    ).add_to(m)

# Show the map
m.save("map.html")
