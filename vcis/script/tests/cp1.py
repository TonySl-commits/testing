from collections import deque
import matplotlib.pyplot as plt
import pandas as pd
from vcis.utils.utils import CDR_Properties, CDR_Utils
import math
import folium


properties = CDR_Properties()
utils = CDR_Utils()
def find_neighboring_grids(grids, step_lat, step_lon):
    # Function to check if two grids are neighbors
    def is_neighbor(grid1, grid2):
        return abs(grid1[0] - grid2[0]) <= step_lat and abs(grid1[1] - grid2[1]) <= step_lon

    visited = set()
    groups = []

    for grid in grids:
        grid_key = (grid['latitude_grid'], grid['longitude_grid'])
        if grid_key not in visited:
            queue = deque([(grid, grid_key)])
            visited.add(grid_key)
            current_group = [grid]

            while queue:
                current_grid, current_key = queue.popleft()
                for neighbor in grids:
                    neighbor_key = (neighbor['latitude_grid'], neighbor['longitude_grid'])
                    if neighbor_key not in visited and is_neighbor((current_grid['latitude_grid'], current_grid['longitude_grid']), (neighbor['latitude_grid'], neighbor['longitude_grid'])):
                        queue.append((neighbor, neighbor_key))
                        visited.add(neighbor_key)
                        current_group.append(neighbor)

            groups.append(current_group)

    return groups

# # Example usage

df = pd.read_csv(properties.passed_filepath_excel + 'df_main.csv')

df = df.drop_duplicates(subset=['latitude_grid','longitude_grid'])
df = df.sort_values(['latitude_grid','longitude_grid'])
grids = df[['latitude_grid', 'longitude_grid']].to_dict('records')
print(grids)
# grids = [
#     {'lat': 10, 'lon': 20},
#     {'lat': 11, 'lon': 21},
#     {'lat': 13, 'lon': 22},
#     {'lat': 15, 'lon': 25},
#     {'lat': 16, 'lon': 26}
# ]

step_lat,step_lon = utils.get_step_size(100*math.sqrt(2))

neighboring_groups = find_neighboring_grids(grids, step_lat, step_lon)
# for i, group in enumerate(neighboring_groups):
#     print(f"Group {i+1}: {[f'({g})' for g in group]}")



def visualize_grids_on_map(groups):
    # Define a list of colors for each group
    colors = ['black', 'darkblue', 'orange', 'red', 'lightgray', 'beige', 'lightred', 'pink', 'white', 'blue', 'darkpurple', 'gray', 'purple', 'lightgreen', 'lightblue', 'green', 'cadetblue', 'darkgreen', 'darkred']
    

    m = folium.Map(location=['33.89540036536618', '35.56827417118015'], zoom_start=13)

    # Assign a unique color to each group and add markers for each grid within the group
    for i, group in enumerate(groups):
        color = colors[i % len(colors)]  # Cycle through colors if there are more groups than colors
        for grid in group:
            folium.Circle(
                location=(grid['latitude_grid'], grid['longitude_grid']),
                color=color,  # Use the assigned color for the marker
                popup=str(grid)  # Display grid information on hover
            ).add_to(m)

    # Optionally, add labels for each group
    for i, group in enumerate(groups):
        folium.Polygon(
            locations=[[grid['latitude_grid'], grid['longitude_grid']] for grid in group],
            color=color,  # Use the assigned color for the polygon
            fill=False,
            weight=2,
            opacity=0.5
        ).add_to(m)

    # Save the map to an HTML file
    m.show_in_browser()
