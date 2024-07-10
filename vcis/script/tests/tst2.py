import folium
from shapely.geometry import Point, Polygon
from vcis.utils.utils import CDR_Properties, CDR_Utils
import math
import folium


properties = CDR_Properties()
utils = CDR_Utils()
# Define the central point and the radius for the buffer
central_point = Point(40.7128, -74.0060)  # New York City coordinates
a,radius = utils.get_step_size(100)

bc = central_point.buffer(radius)

exterior_coords = bc.exterior.coords

# Create a base map centered around the first coordinate of the polygon
m = folium.Map(location=list(exterior_coords)[0], zoom_start=4)

# Add the polygon to the map
folium.Polygon(locations=list(exterior_coords), color='blue', fill=True, fill_color='blue', fill_opacity=0.5, stroke=True, stroke_color='blue', stroke_weight=2).add_to(m)


m.show_in_browser()