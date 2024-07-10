import pandas as pd
from vcis.utils.utils import CDR_Utils,CDR_Properties 

properties = CDR_Properties() 
utils = CDR_Utils()
df = pd.read_csv(properties.passed_filepath_excel + 'df_merged_devices.csv')
# df  = df[df['device_id_y'] == '9ca9ca7d-c84b-479e-a4dc-c732e47f5a32'].reset_index(drop=True)
# print()
distance = +
step_lat, step_lon = utils.get_step_size(distance=distance)
def uncommon_time_list(group):
    changes = []
    start = None
    prev_row = None
    end = None
    for _, row in group.iterrows():
        x=0
        if prev_row is None:
            prev_row = row
        if start is None: #ONE TIME
            # print(row['latitude_grid_x']-step_lat , row['latitude_grid_y'] , row['latitude_grid_x']+step_lat)
            if not ((row['latitude_grid_x']-step_lat < row['latitude_grid_y'] < row['latitude_grid_x']+step_lat) and (row['longitude_grid_x']-step_lon < row['longitude_grid_y'] < row['longitude_grid_x']+step_lon)):
                start = row['usage_timeframe']
                x=x+1
        elif start is not None and ((row['latitude_grid_x']-step_lat <= row['latitude_grid_y'] <= row['latitude_grid_x']+step_lat) and (row['longitude_grid_x']-step_lon <= row['longitude_grid_y'] <= row['longitude_grid_x']+step_lon)):
            end = prev_row['usage_timeframe']
            changes.append([start, end])
            start = None
            end=None
        prev_row = row
    if start is not None and end is None:
        changes.append([start, group['usage_timeframe'].iloc[-1]])
    return changes

results = df.groupby('device_id_y').apply(uncommon_time_list)


print(results)

