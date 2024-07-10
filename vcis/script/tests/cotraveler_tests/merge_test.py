import pandas as pd
import numpy as np
from IPython.display import display


def lcs(sequence1, sequence2):
    if len(sequence1) != len(sequence2):
        raise ValueError("Sequences have different lengths")
    #array of empty tuples
    lcs_array = [0] 
    m = len(sequence1)

    sequence1 = [tuple(seq) for seq in sequence1]
    sequence2 = [tuple(seq) for seq in sequence2]
    
    for i, j in zip(range(1, m+1), range(1, m+1)):
        if sequence1[i-1] == sequence2[j-1]:
            print(sequence1[i-1], " =" ,sequence2[j-1])
            lcs_array.append((lcs_array[i-1]+1))
        else:
            # print(sequence1[i-1], " != " ,sequence2[j-1])
            lcs_array.append(0)
        # if i%50 == 0:
        #     print("lcs_array:",lcs_array)
    return max(lcs_array)
  
data1 = pd.DataFrame({
  'device_id': ["a"],
  'usage_timeframe': [1704290400000],
  'location_latitude': [1],
  'location_longitude': [1]
})

# def convert_date_to_timestamp(data,date_column:str = 'usage_timeframe'):
#     data[date_column] = [
#         int(timestamp.timestamp() * 1000) for timestamp in data[date_column]
#     ]
#     return data
# Convert 'usage_timeframe' column to datetime
# data1['usage_timeframe'] = pd.to_datetime(pd.to_numeric(data1['usage_timeframe'], errors='coerce'), unit='ms')
# data1 = data1.sort_values(by='usage_timeframe')
# data1.index = data1['usage_timeframe']
b= int(1704290400000- (10.0*60*1000))
data2 = pd.DataFrame({
  'device_id': ["b"],
  'usage_timeframe': [b],
  'location_latitude': [1],
  'location_longitude': [1]
})

# data2['usage_timeframe'] = pd.to_datetime(pd.to_numeric(data2['usage_timeframe'], errors='coerce'), unit='ms', origin=pd.Timestamp('1970-01-01'))

# data2 = data2.sort_values(by='usage_timeframe')
# data2.index = data2['usage_timeframe']   

tol = 10*60*1000

df_merged = pd.merge_asof(left=data1,
                          right=data2,
                          on='usage_timeframe',
                          direction='nearest',
                          tolerance=tol).dropna()

display(df_merged[['device_id_x',
                      'location_latitude_x','device_id_y',
                      'location_latitude_y']])

df_main = df_merged[['device_id_x',
                      'location_latitude_x',
                      'location_longitude_x',
                      'usage_timeframe']]

df_devices = df_merged[['device_id_y',
                    'location_latitude_y',
                    'location_longitude_y',
                    'usage_timeframe']]

df_main.columns = df_main.columns.str.replace('_x', '')
df_devices.columns = df_devices.columns.str.replace('_y', '')
df_devices = df_devices.reset_index(drop=True)

df_devices['sequence'] = df_devices[['location_latitude', 'location_longitude']].apply(np.array, axis=1)
df_main['sequence'] = df_main[['location_latitude', 'location_longitude']].apply(np.array, axis=1)

devices_sequence_list = df_devices['sequence'].values
main_sequence_list = df_main['sequence'].values

max = lcs(devices_sequence_list,main_sequence_list)

print("Max:",max)