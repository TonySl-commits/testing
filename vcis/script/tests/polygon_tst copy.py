from vcis.utils.utils import CDR_Properties, CDR_Utils
import pandas as pd
from math import atan2, pi
import numpy as np
from shapely.geometry import Point, Polygon, LineString, MultiPoint, MultiPolygon

properties = CDR_Properties()
utils = CDR_Utils()
df = pd.read_csv(properties.passed_filepath_excel + 'df_main.csv')
df = df.head(50)
from shapely.geometry import Point, Polygon
from shapely.ops import unary_union
from rtree import index

def create_rtree_index(points):
    """
    Create an Rtree index for efficient spatial queries.
    """
    idx = index.Index()
    for i, point in enumerate(points):
        idx.insert(i, point.bounds)
    return idx

def find_intersections_using_rtree(idx, points):
    """
    Find groups of intersecting points using Rtree.
    Returns a list of lists, where each inner list contains indices of intersecting points.
    """
    intersections = []
    for i in range(len(points)):
        # Get indices of points intersecting with the current point
        intersecting_indices = [j for j in idx.intersection(points[i].bounds) if j!= i]
        if intersecting_indices:
            intersections.append(intersecting_indices)
    return intersections


# def create_polygons(intersections, points):
#     """
#     Create polygons for each group of intersecting points.
#     Returns a list of polygons.
#     """
#     polygons = []
#     for group_indices in intersections:
#         # Sort indices to ensure consistent ordering
#         sorted_group_indices = sorted(group_indices)
        
#         # Extract points corresponding to the group
#         group_points = [points[i] for i in sorted_group_indices]
        
#         # Determine the type of geometry to create based on the number of points
#         if len(group_points) == 3:
#             # Create a triangle
#             polygon = Polygon(group_points)
#         elif len(group_points) == 2:
#             # Create a line segment
#             polygon = LineString(group_points)
#         elif len(group_points) == 1:
#             # Create a circle around the point
#             center = group_points[0]
#             radius = 1  # Adjust the radius as needed
#             circle = center.buffer(radius)
#             polygon = circle
#         else:
#             # For groups with more than 3 points, create a polygon
#             polygon = Polygon(group_points)
        
#         polygons.append(polygon)
    
#     return polygons
def create_polygons(intersections, points):
    """
    Create polygons for each group of intersecting points.
    Returns a list of polygons.
    """
    polygons = []
    for group_indices in intersections:
        # Sort indices to ensure consistent ordering
        sorted_group_indices = sorted(group_indices)
        
        # Extract points corresponding to the group
        group_points = [points[i] for i in sorted_group_indices]
        
        # Check if the group has at least 4 points
        if len(group_points) >= 4:
            # Create a polygon from the group of points
            polygon = Polygon(group_points)
            polygons.append(polygon)
        else:
            print(f"Skipping group with {len(group_points)} points.")
            # Depending on your needs, you might choose to continue with the next iteration
            # or handle this case differently
            
    return polygons

# Example usage
points = [Point(x, y) for x, y in zip(df['longitude_grid'], df['latitude_grid'])]
idx = create_rtree_index(points)
intersections = find_intersections_using_rtree(idx, points)
polygons = create_polygons(intersections, points)


print(polygons)

import folium
import geopandas as gpd
from shapely.geometry import Point, Polygon
from shapely.ops import unary_union
from rtree import index

# Assuming the rest of your code remains unchanged up to the creation of polygons

# Convert polygons to GeoDataFrame
gdf_polygons = gpd.GeoDataFrame(geometry=polygons)

# Create a base map centered around the mean latitude and longitude of your points
map_center = (np.mean(df['latitude_grid']), np.mean(df['longitude_grid']))
m = folium.Map(location=map_center, zoom_start=10)

for _, row in gdf_polygons.iterrows():
    # Convert the Polygon object to a list of its vertices
    polygon_vertices = list(row.geometry.exterior.coords)
    folium.Polygon(polygon_vertices, color='blue', fill=True, fill_color='lightblue').add_to(m)


# Save the map to an HTML file
m.show_in_browser()