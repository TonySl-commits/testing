import pandas as pd
import pickle
import networkx as nx
import plotly.graph_objects as go
import plotly.express as px
import random

from matplotlib.colors import to_hex
from matplotlib.colors import LinearSegmentedColormap


def create_random_df(num_devices, num_edges):
    # Generate device names
    devices = [f"Device{i+1}" for i in range(num_devices)]

    # Generate random connections and visits
    data = {
        'device_id': random.choices(devices, k=num_edges),
        'main_id': random.choices(devices, k=num_edges),
        'common_visits': [random.randint(1, 10) for _ in range(num_edges)]
    }

    df = pd.DataFrame(data)

    return df


df = create_random_df(num_devices=50, num_edges=100)



def read_dictionary(filepath):
    with open(filepath + "common_df_dict.pickle", 'rb') as file:
        common_df_dict = pickle.load(file)

    common_dfs = []
    for key, df in common_df_dict.items():
        df['main_id'] = key
        common_dfs.append(df)

    # Concatenate all common_dfs into one
    result_df = pd.concat(common_dfs, ignore_index=True)

    return result_df
