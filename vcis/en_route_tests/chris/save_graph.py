import osmnx as ox

# Configure OSMnx to log some useful stats
ox.config(log_console=True, use_cache=True)

# Download the graph for Lebanon
place_name = "Lebanon"
graph = ox.graph_from_place(place_name, network_type='drive', simplify=False)  # You can change 'drive' to 'walk', 'bike', or 'all' depending on your needs

# Save the graph to a file (GraphML format)
filename = "Lebanon.graphml"
ox.io.save_graphml(graph, filename)
print(f"Graph for {place_name} saved as {filename}")
