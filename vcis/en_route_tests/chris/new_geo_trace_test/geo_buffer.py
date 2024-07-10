import osmnx as ox
import geopandas as gpd
from shapely.geometry import Point, LineString
from shapely.ops import nearest_points
import folium

from geopy.distance import geodesic

# Define your point of interest
# Latitude = 33.86181411504806
# Longitude = 35.550111291014225
Latitude = 33.87418186564064
Longitude = 35.537983117274976
point_of_interest = Point(Longitude, Latitude)

# Load the graph and convert to GeoDataFrame
G = ox.graph_from_point((Latitude, Longitude), dist=1000, network_type='drive', simplify=False)
edges = ox.graph_to_gdfs(G, nodes=False, edges=True)

# Create a buffer around your point if needed (optional, for broader searches)
buffer_radius = 0.001 
buffer = point_of_interest.buffer(buffer_radius)

# Find edges within the buffer
potential_edges = edges[edges.intersects(buffer)].copy()

# Calculate the nearest edge explicitly
def calculate_distance(row):
    # Retrieve the nearest point on the line to the point of interest
    line = row.geometry
    nearest_geom = nearest_points(point_of_interest, line)[1]  # This gives you the closest point on the edge

    # Use geodesic distance which calculates distance on the surface of a sphere
    distance = geodesic((point_of_interest.y, point_of_interest.x), (nearest_geom.y, nearest_geom.x)).meters
    distance = round(distance, 2)
    return distance


potential_edges.loc[:, 'distance'] = potential_edges.apply(calculate_distance, axis=1)
closest_edges = potential_edges.nsmallest(5, 'distance')  # Get the 5 closest edges

closest_edges.reset_index(inplace=True)
closest_edges.drop(columns=['key', 'bridge', 'oneway', 'lanes', 'ref', 'name', 'highway', 'reversed', 'length', 'maxspeed', 'junction', 'tunnel'], inplace=True)

print("Closest edges to your point:\n", closest_edges)

print(closest_edges.iloc[0])
print(dict(closest_edges.iloc[0]))

# # Visualization using Folium (if needed)
# m = folium.Map(location=[Latitude, Longitude], zoom_start=15)
# folium.Marker([Latitude, Longitude], popup='Point of Interest', icon=folium.Icon(color='red')).add_to(m)

# # Add closest edges to the map
# for _, row in closest_edges.iterrows():
#     line = [(lat, lon) for lon, lat in row.geometry.coords]  # Correctly unpack coordinates
#     folium.PolyLine(line, color='blue', weight=5).add_to(m)

# m.save("map4.html")

# Get the average distance
average_distance = closest_edges['distance'].mean().round(2)
print(average_distance)