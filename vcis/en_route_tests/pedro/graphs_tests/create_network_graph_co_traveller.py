import pandas as pd
import networkx as nx
import plotly.graph_objects as go
import numpy as np
from matplotlib.colors import LinearSegmentedColormap, to_hex

# Load your data
df_common = pd.read_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/co_traveller/df_common.csv")

def get_node_graph_co_traveller(df_common):

    # Normalize device_id and main_id to ensure each pair is unique regardless of order
    df_common[['device_id', 'main_id']] = pd.DataFrame(np.sort(df_common[['device_id', 'main_id']], axis=1), index=df_common.index)

    # Convert timestamps to datetime and extract date
    df_common['date'] = pd.to_datetime(df_common['usage_timeframe'], unit='ms').dt.date

    # Now, you can proceed with your aggregation
    df_interactions = df_common.groupby(['device_id', 'main_id']).agg(
        common_hits=('usage_timeframe', 'size'),  # Count of total interactions
        common_days=('date', 'nunique')  # Count of unique interaction days
    ).reset_index()

    # Calculate total hits for normalization
    total_hits = df_interactions.groupby('device_id')['common_hits'].transform('sum')

    # Calculate the suspicious percentage based on interactions over total hits
    df_interactions['suspicious'] = df_interactions['common_hits'] / total_hits * 100

    # Define a color map based on suspicious levels
    def get_color(suspicious):
        if suspicious <= 25:
            return '#000000'  # Black
        elif suspicious <= 50:
            return '#550000'  # Darker Red
        elif suspicious <= 75:
            return '#aa0000'  # Medium Red
        else:
            return '#ff0000'  # Bright Red

    # Apply color map to interactions
    df_interactions['color'] = df_interactions['suspicious'].apply(get_color)

    # Create the graph
    G = nx.from_pandas_edgelist(df_interactions, 'device_id', 'main_id', edge_attr=True)
    pos = nx.spring_layout(G, k=0.3)

    # Plotting
    fig = go.Figure()

    # Add edges
    for edge in G.edges(data=True):
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        color = edge[2]['color']
        fig.add_trace(go.Scatter(x=[x0, x1, None], y=[y0, y1, None],
                                mode='lines', line=dict(color=color, width=2), 
                                hoverinfo='text', text=f'Common Days: {edge[2]["common_days"]}, Suspicious: {edge[2]["suspicious"]:.2f}%'))

    # Add nodes
    for node in G.nodes():
        fig.add_trace(go.Scatter(x=[pos[node][0]], y=[pos[node][1]],
                                mode='markers', marker=dict(color='black', size=12),
                                text=node, hoverinfo='text'))
        
    # Define the colors and labels for the legend
    colors = ['#000000', '#550000', '#aa0000', '#ff0000']

    # Create the color scale
    min_percentage = 0
    max_percentage = 100

    # Add a color bar
    fig.add_trace(go.Scatter(
        x=[None],
        y=[None],
        mode='markers',
        marker=dict(
            size=10,
            color=[min_percentage, max_percentage],  # Provide a range of values
            colorscale=colors,
            colorbar=dict(title='Suspicious Percentage', titleside='right'),
            showscale=True
        ),
        hoverinfo='text',
        text='Color scale indicates the level of suspicious activity percentage'  # Example hover text
    ))

    fig.update_layout(title="Network Graph of Device Connections", showlegend=False)
    fig.show()


get_node_graph_co_traveller(df_common)