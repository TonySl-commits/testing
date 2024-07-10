import folium
import pandas as pd
from vcis.utils.utils import CDR_Utils, CDR_Properties
from branca.element import Element

utils = CDR_Utils()
properties = CDR_Properties()

# Load the data from the CSV file
df_common = pd.read_csv(properties.passed_filepath_excel + 'df_common.csv')

# Create a map centered at a specific location (you can adjust the coordinates as needed)
m = folium.Map(location=[34.06, -118.25], zoom_start=8)

# Convert the DataFrame to a list of dictionaries for use in JavaScript
devices = df_common.to_dict('records')

# Your custom JavaScript code
js_code = f"""
<script>
document.addEventListener('DOMContentLoaded', function() {{
    // Initialize the map
    var map = L.map('map').setView([34.06, -118.25], 8); // Set the initial view to a specific location

    // Add a tile layer to the map
    L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    }}).addTo(map);

    // Sample data for devices
    var devices = {devices};

    // Add circles for each device
    devices.forEach(function(device) {{
        L.circle([device.location_latitude, device.location_longitude], {{
            color: 'blue',
            fillColor: '#0000ff',
            fillOpacity: 0.5,
            radius: 500 // Adjust the radius as needed
        }}).addTo(map)
        .bindPopup("<b>Device ID:</b> " + device.device_id + "<br><b>Usage Timeframe:</b> " + device.usage_timeframe);
    }});
}});
</script>
"""

# Create an Element with your JavaScript code
e = Element(js_code)

# Add the JavaScript code to the map
m.get_root().html.add_child(e)

# Display the map with the added JavaScript
m.show_in_browser()