import pandas as pd

# Sample DataFrame
data = {
    'latitude_grid': [33.896757, 33.898566, 33.893139, 33.894948, 33.896757],
    'longitude_grid': [35.495305, 35.495305, 35.508278, 35.495305, 35.497467],
    'device_ids': [
        ['bb9448a9-edf8-45ef-acab-f0b0be8df09c', '85f3005...'],
        ['bb9448a9-edf8-45ef-acab-f0b0be8df09c', '85f3005...'],
        ['e943a632-5936-4e48-a3d3-adba829ea3c1', 'af8994d...'],
        ['85f30053-af3c-4a92-a281-ab3b3e71b9ff', '24ef5d9...'],
        ['E0222C00-CE50-4B7F-AE50-20880871EA9B', 'bb9448a...']
    ],
    'location': [
        (33.896756922965615, 35.49530529323608),
        (33.89856566643153, 35.49530529323608),
        (33.893139436033785, 35.50827753820391),
        (33.8949481794997, 35.49530529323608),
        (33.896756922965615, 35.49746733406405)
    ]
}
df = pd.DataFrame(data)


import networkx as nx

# Create an empty graph
G = nx.Graph()

# Add nodes and edges
for index, row in df.iterrows():
    # Add each device as a node
    for device_id in row['device_ids']:
        G.add_node(device_id)
    # Add an edge for each location
    for i in range(len(row['device_ids'])):
        for j in range(i+1, len(row['device_ids'])):
            G.add_edge(row['device_ids'][i], row['device_ids'][j], location=row['location'])

import matplotlib.pyplot as plt

# Draw the graph
nx.draw(G, with_labels=True)
plt.savefig('graph.png', dpi=100, bbox_inches='tight')