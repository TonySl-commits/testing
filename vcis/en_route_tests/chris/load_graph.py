import osmnx as ox
import matplotlib.pyplot as plt
import time 

# Configure OSMnx to log some useful stats
ox.config(log_console=True, use_cache=True)

def load_graph_from_bbox(graph_file, north, south, east, west):

    print('INFO:    STARTED LOADING THE GRAPHML FILE !!!')
    # Load the full graph from file
    graph = ox.load_graphml(graph_file)

    print('INFO:    FINISHED LOADING THE GRAPHML FILE !!!')

    # Extract the subgraph within the specified bounding box
    subgraph = ox.truncate.truncate_graph_bbox(graph, bbox=(north, south, east, west))
    
    print('INFO:    FINISHED TRUNCATING THE GRAPHML FILE !!!')

    return subgraph

# Example bounding box for Beirut, Lebanon
north, south, east, west = 33.900, 33.871, 35.548, 35.486

# Save the graph to a file (GraphML format)
filename = "Lebanon.graphml"

start_time = time.time()

# Load subgraph from the bounding box
subgraph = load_graph_from_bbox(filename, north, south, east, west)

end_time = time.time()

print('INFO:  Time passed -->', end_time - start_time, 'seconds.')

# def visualize_graph(graph, filename):
#     # Plot the graph
#     fig, ax = ox.plot_graph(graph, show=False, close=False)  # Set show=False to not display the plot window, close=False to keep it open for saving
    
#     # Save the figure
#     fig.savefig(filename, dpi=300)  # Save as PNG with high resolution
#     plt.close(fig)  # Close the plot figure to free up memory


# visualize_graph(subgraph, 'lebanon_drive_graph.png')
