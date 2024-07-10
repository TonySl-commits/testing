import pandas as pd
import numpy as np
from vcis.utils.utils import CDR_Utils, CDR_Properties
from vcis.cotraveler.cotraveler_functions import CotravelerFunctions
utils = CDR_Utils()
properties =CDR_Properties()
cotraveler_functions = CotravelerFunctions()
df_device = pd.read_csv(properties.passed_filepath_excel+'df_device.csv')
df_cotraveler = pd.read_csv(properties.passed_filepath_excel+'df_cotraveler.csv')

df_device = df_device.reset_index(drop=True).sort_values('usage_timeframe')
df_cotraveler = df_cotraveler.reset_index(drop=True).sort_values('usage_timeframe')
df_device = utils.convert_datetime_to_ms(df_device)

minutes = 10
tol = minutes * 60 * 1000

df_device['usage_timeframe'] = df_device['usage_timeframe'].astype(np.int64)
df_cotraveler['usage_timeframe'] = df_cotraveler['usage_timeframe'].astype(np.int64)

df_merged = pd.merge_asof(left=df_device,
                          right=df_cotraveler,
                          on='usage_timeframe',
                          direction='nearest',
                          tolerance=tol).dropna()
df_merged.to_csv(properties.passed_filepath_excel+'df_merged.csv',index=False)

df_device = df_merged[['device_id_x',
                        'location_latitude_x',
                        'location_longitude_x',
                        'usage_timeframe',
                        'latitude_grid_x',
                        'longitude_grid_x',
                        'grid_x']]

df_cotraveler = df_merged[['device_id_y',
                    'location_latitude_y',
                    'location_longitude_y',
                    'usage_timeframe',
                    'latitude_grid_y',
                    'longitude_grid_y',
                    'grid_y']]

df_device.columns = df_device.columns.str.replace('_x', '')
df_cotraveler.columns = df_cotraveler.columns.str.replace('_y', '')

df_cotraveler = df_cotraveler.reset_index(drop=True)
df_device = df_device.reset_index(drop=True)

# getting df_cotraveler
df_cotraveler =df_cotraveler.sort_values('usage_timeframe')
df_device = df_device.sort_values('usage_timeframe')

# print(df_device[['usage_timeframe','grid']],df_cotraveler[['usage_timeframe','grid']])
devices_sequence_list = df_cotraveler['grid'].str.split(',').apply(lambda x: [float(i) for i in x]).tolist()
main_sequence_list = df_device['grid'].str.split(',').apply(lambda x: [float(i) for i in x]).tolist()

def lcs(sequence1, sequence2):

    if len(sequence1) != len(sequence2):
        raise ValueError("Sequences have different lengths")
    #array of empty tuples
    lcs_array = [0] 
    m = len(sequence1)
    for i, j in zip(range(1, m+1), range(1, m+1)):
        if sequence1[i-1] == sequence2[j-1]:
            lcs_array.append((lcs_array[i-1]+1))
            
        elif sequence1[i-1] != sequence2[j-1]:
            lcs_array.append(0)
        else:
            print("Error")
    return max(lcs_array)

max_value = lcs(devices_sequence_list,main_sequence_list)
print(f"The max value is: {max_value}")
