# import folium.map 
from folium.plugins import MarkerCluster
import folium
from jinja2 import Template
from folium.map import Marker
import numpy as np

size = 100
lons = np.random.randint(-180, 180, size=size)
lats = np.random.randint(-90, 90, size=size)

locations = list(zip(lats, lons))
# Modify Marker template to include the onClick event
click_template = """{% macro script(this, kwargs) %}
    var {{ this.get_name() }} = L.marker(
        {{ this.location|tojson }},
        {{ this.options|tojson }}
    ).addTo({{ this._parent.get_name() }}).on('click', onClick);
{% endmacro %}"""

# Change template to custom template
Marker._template = Template(click_template)
print(Marker._template)
location_center = [51.7678, -0.00675564]
m = folium.Map(location_center, zoom_start=13)

# Create the onClick listener function as a branca element and add to the map html
click_js = """function onClick(e) {
                 var point = e.latlng; alert(point)
                 }"""
                 
e = folium.Element(click_js)
html = m.get_root()
html.script.get_root().render()
html.script._children[e.get_name()] = e

#Add marker (click on map an alert will display with latlng values)
marker = MarkerCluster(locations = locations).add_to(m)
m.save('test.html')


# import folium
# from jinja2 import Template
# from folium.plugins import MarkerCluster

# # Custom template for MarkerCluster including onClick event
# click_cluster_template = """
# {% macro script(this, kwargs) %}
# var {{ this.get_name() }} = L.markerClusterGroup({
#     {{ this.options|tojson }}
# }).addTo({{ this._parent.get_name() }});

# // Adding onClick event to the cluster
# this.on('clusterclick', function(e) {
#     var cluster = e.layer;
#     if(cluster.getAllChildMarkers().length > 0){
#         // Handle click on child markers inside the cluster
#         console.log("Cluster clicked");
#     } else {
#         // Handle direct click on the cluster
#         console.log("Direct cluster click");
#     }
# });
# {% endmacro %}
# """

# # Change template to custom template
# MarkerCluster._template = Template(click_cluster_template)

# location_center = [51.7678, -0.00675564]
# m = folium.Map(location_center, zoom_start=13)

# # Create the MarkerCluster and add it to the map
# mc = MarkerCluster(locations=locations).add_to(m)

# # Optionally, add markers to the cluster
# # mc.add_child(folium.Marker([51.7678, -0.00675564]))

# # Define the onClick function as a Branca element and add to the map HTML
# click_js = """function onClick(e) {
#                  var point = e.latlng; alert(point)
#                  }"""
# e = folium.Element(click_js)
# html = m.get_root()
# html.script.get_root().render()
# html.script._children[e.get_name()] = e

# # Display the map
# m.save('test.html')


