import plotly.graph_objects as go
import numpy as np
import pandas as pd

data = pd.read_csv(r'C:\Users\zriyad\Desktop\CDR_Trace\src\data\excel\df_history.csv')

def lcs(sequence1, sequence2):

    if len(sequence1) != len(sequence2):
        raise ValueError("Sequences have different lengths")
    lcs_array = [0] 
    m = len(sequence1)

    sequence1 = [tuple(seq) for seq in sequence1]
    sequence2 = [tuple(seq) for seq in sequence2]
    for i, j in zip(range(1, m+1), range(1, m+1)):
        if sequence1[i-1] == sequence2[j-1]:
            lcs_array.append((lcs_array[i-1]+1))
        else:
            lcs_array.append(0)

    return max(lcs_array) 
def get_heat_matrix(df_history,tol=5):
    device_ids = df_history['device_id'].unique()
    lcs_matrix = np.zeros((len(device_ids), len(device_ids)))
    for i, device_id in enumerate(device_ids):
        df_device = df_history[df_history['device_id'] == device_id]
        for j, device_id_co in enumerate(device_ids):
            df_device_co = df_history[df_history['device_id'] == device_id_co]    
            
            df_merged = pd.merge_asof(left=df_device,right=df_device_co,on='usage_timeframe',direction='nearest', tolerance=tol).dropna()
            device_id_sequence = df_merged[['device_id_x','location_latitude_x','location_longitude_x','usage_timeframe','latitude_grid_x','longitude_grid_x','grid_x']]
            device_id_co_sequence = df_merged[['device_id_y','location_latitude_y','location_longitude_y','usage_timeframe','latitude_grid_y','longitude_grid_y','grid_y']]
            device_id_sequence.columns = device_id_sequence.columns.str.replace('_x', '')
            device_id_co_sequence.columns = device_id_co_sequence.columns.str.replace('_y', '')
            device_id_co_sequence = device_id_co_sequence.reset_index(drop=True)
            device_id_sequence = device_id_sequence.reset_index(drop=True)
            device_id_co_sequence =device_id_co_sequence.sort_values('usage_timeframe')
            device_id_sequence = device_id_sequence.sort_values('usage_timeframe')

            device_id_co_sequence = device_id_co_sequence['grid'].str.split(',').apply(lambda x: [float(i) for i in x]).tolist()
            device_id_sequence = device_id_sequence['grid'].str.split(',').apply(lambda x: [float(i) for i in x]).tolist()

            lcs_value = lcs(device_id_sequence,device_id_co_sequence)
            
            lcs_matrix[i, j] = lcs_value

    n= len(lcs_matrix)
    print(n)
    for i in range(n-1,1,1):
        for j in range(n-1,1,1):
            if i>j:
                lcs_matrix[i][j] =lcs_matrix[i][j]/lcs_matrix[j][j]
            else:
                lcs_matrix[i][j] =lcs_matrix[i][j]/lcs_matrix[i][i]

    fig = go.Figure(data=go.Heatmap(z=lcs_matrix, x=device_ids, y=device_ids,colorscale = 'Blues'))
    fig.update_layout(xaxis=dict(title='Device IDs'),yaxis=dict(title='Device IDs'))
    fig.show()

get_heat_matrix(data)