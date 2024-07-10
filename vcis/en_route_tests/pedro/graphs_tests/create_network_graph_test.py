import pandas as pd
import networkx as nx
import plotly.graph_objects as go
import plotly.express as px
import random

from matplotlib.colors import to_hex
from matplotlib.colors import LinearSegmentedColormap

df_common = pd.read_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/co_traveller/df_common.csv")

def create_network_graph(df_common):

    df = df_common.groupby(['device_id', 'main_id']).size().reset_index(name='common_visits')

    # Ensuring no self-connections and no repeated edges
    df = df[df['device_id'] != df['main_id']]
    df.drop_duplicates(subset=['device_id', 'main_id'], keep='first', inplace=True)

    # Create the graph
    G = nx.from_pandas_edgelist(df, 'device_id', 'main_id', 'common_visits')
    pos = nx.spring_layout(G)

    # Define the colors for the gradient; dark blue to light blue
    colors = ["#03045E", "#90E0EF"]

    # Create a new colormap from the listed colors
    edge_color_scale = LinearSegmentedColormap.from_list("custom_blue", colors, N=256)

    # Generate a Plotly colorscale from sampled colors
    max_visits = max(df['common_visits'])
    min_visits = min(df['common_visits'])

    # Create a list of colors from the colormap
    num_colors = 256  # Number of colors to sample
    sampled_colors = [to_hex(edge_color_scale(i / num_colors)) for i in range(num_colors + 1)]

    # Generate a Plotly colorscale from sampled colors
    plotly_colorscale = [[i / num_colors, color] for i, color in enumerate(sampled_colors)]

    # Use this colorscale for edges and nodes in your graph
    fig = go.Figure()

    # Edge traces with hover info and color scale
    max_visits = max(df['common_visits'])

    i = 0
    for edge in G.edges(data=True):
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        visits = edge[2]['common_visits']
        color_index = int(visits / max_visits * num_colors)
        color = plotly_colorscale[color_index][1]
        fig.add_trace(go.Scatter(
            x=[x0, x1, None], y=[y0, y1, None],
            mode='lines',
            line=dict(width=2, color=color),
            hoverinfo='text',
            text=f'Common Visits: {visits}',
            showlegend=True
        ))

    # Node traces with hover info and size/color scale
    node_adjacencies = [len(list(G.neighbors(node))) for node in G.nodes()]
    node_text = [f'Device: {node}<br>#Connections: {len(list(G.neighbors(node)))}' for node in G.nodes()]

    fig.add_trace(go.Scatter(
        x=[pos[node][0] for node in G.nodes()],
        y=[pos[node][1] for node in G.nodes()],
        mode='markers',
        marker=dict(
            color="#333333",  # Node color depends on degree
            size=12,
            # colorscale=plotly_colorscale,  # Using the custom colorscale
            # colorbar=dict(title="Node Degree"),
            line=dict(width=2, color='#333333')
            # showscale=True
        ),
        text=node_text,
        hoverinfo='text'
    ))

    # Add a color bar
    fig.add_trace(go.Scatter(
        x=[None],
        y=[None],
        mode='markers',
        marker=dict(
            size=10,
            color=[min_visits, max_visits],  # Provide a range of values
            colorscale=colors,
            colorbar=dict(title='Common Visits', titleside='right'),
            showscale=True
        ),
        hoverinfo='none'
    ))

    # Update layout
    fig.update_layout(
        title='Network Graph of Device Connections',
        showlegend=False,
        hovermode='closest',
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
    )

    fig.show()


create_network_graph(df_common)