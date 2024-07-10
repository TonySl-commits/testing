
import pandas as pd
import plotly.graph_objects as go
import folium
from vcis.utils.utils import CDR_Utils, CDR_Properties
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
from folium.plugins import HeatMap


##########################################################################################################

utils = CDR_Utils(verbose=True)
properties = CDR_Properties()

df_merged_list = pd.read_csv(properties.passed_filepath_excel+'df_merged_devices.csv')
df_main = pd.read_csv(properties.passed_filepath_excel+'df_device.csv')
df_history = pd.read_csv(properties.passed_filepath_excel+'df_history.csv')
df_common = pd.read_csv(properties.passed_filepath_excel+'df_common.csv')
df_merged_list = pd.read_csv(properties.passed_filepath_excel+'df_merged_list.csv')
df_common = pd.read_csv(properties.passed_filepath_excel +'df_common.csv')

pd.set_option('display.max_columns', None)
df_common = utils.convert_datetime_to_ms(df_common)
df_common = df_common.drop_duplicates(subset=['device_id','usage_timeframe'])
df_common = df_common[['device_id', 'location_latitude', 'location_longitude','usage_timeframe','grid']]
df_history = df_history[['device_id', 'location_latitude', 'location_longitude','usage_timeframe','grid']]

# x=0
# for device_id in df_history['device_id'].unique():
#     df_device = df_history[df_history['device_id'] == device_id]
#     df_device_common = df_common[df_common['device_id'] ==device_id]
#     if x==32:
#         break
#     x=x+1
df_device=  df_history[df_history['device_id'] == '47bc4a71-6d1a-41a6-aba6-2a0c8dd23757']
df_device = df_device.reset_index(drop=True)

step_lat,step_lon = utils.get_step_size(50)

main_df_visit = utils.get_visit_df(df_main,step_lat,step_lon)
cotraveler_df_visit = utils.get_visit_df(df_device,step_lat,step_lon)


cotraveler_df_visit = utils.binning(cotraveler_df_visit,50)
main_df_visit = utils.binning(main_df_visit,50)

cotraveler_df_visit['time_difference'] = cotraveler_df_visit['end_time'] - cotraveler_df_visit['start_time']
cotraveler_df_visit = cotraveler_df_visit[cotraveler_df_visit['time_difference'] >= pd.Timedelta(minutes=1)]

main_df_visit['time_difference'] = main_df_visit['end_time'] - main_df_visit['start_time']
main_df_visit = main_df_visit[main_df_visit['time_difference'] >= pd.Timedelta(minutes=1)]

main_df_visit = utils.convert_datetime_to_ms(main_df_visit,'start_time')
cotraveler_df_visit = utils.convert_datetime_to_ms(cotraveler_df_visit,'start_time')

main_df_visit = main_df_visit.sort_values('start_time')
cotraveler_df_visit = cotraveler_df_visit.sort_values('start_time')

main_df_visit['start_time_temp'] = main_df_visit['start_time']
cotraveler_df_visit['start_time_temp'] = cotraveler_df_visit['start_time']
tol = int(5 * 6e+7)

df_merged = pd.merge_asof(left=main_df_visit,
                        right=cotraveler_df_visit,
                        on='start_time_temp',
                        direction='nearest',
                        tolerance=tol).dropna()

main_df_visit_common = df_merged[['device_id_x',
                        'location_latitude_x',
                        'location_longitude_x',
                        'start_time_x',
                         'end_time_x',
                        'latitude_grid_x',
                        'longitude_grid_x',
                        'grid_x']]
cotraveler_df_visit_common = df_merged[['device_id_y',
                    'location_latitude_y',
                    'location_longitude_y',
                    'start_time_y',
                    'end_time_y',
                    'latitude_grid_y',
                    'longitude_grid_y',
                    'grid_y']]

main_df_visit_common.columns = main_df_visit_common.columns.str.replace('_x', '')
cotraveler_df_visit_common.columns = cotraveler_df_visit_common.columns.str.replace('_y', '')

main_df_visit_common = main_df_visit_common.reset_index(drop=True)
cotraveler_df_visit_common = cotraveler_df_visit_common.reset_index(drop=True)

main_df_visit_common = main_df_visit_common.sort_values('start_time')
cotraveler_df_visit_common =cotraveler_df_visit_common.sort_values('start_time')

df_merged = pd.merge(main_df_visit_common,cotraveler_df_visit_common, on=['latitude_grid', 'longitude_grid'], how='inner')

main_df_visit_mask = df_merged[['device_id_x',
                        'location_latitude_x',
                        'location_longitude_x',
                        'start_time_x',
                         'end_time_x',
                        'latitude_grid',
                        'longitude_grid',
                        'grid_x']]

cotraveler_df_visit_mask = df_merged[['device_id_y',
                    'location_latitude_y',
                    'location_longitude_y',
                    'start_time_y',
                    'end_time_y',
                    'latitude_grid',
                    'longitude_grid',
                    'grid_y']]

main_df_visit_mask.columns = main_df_visit_mask.columns.str.replace('_x', '')
cotraveler_df_visit_mask.columns = cotraveler_df_visit_mask.columns.str.replace('_y', '')

main_df_visit_mask = main_df_visit_mask.drop_duplicates().reset_index(drop=True)
cotraveler_df_visit_mask = cotraveler_df_visit_mask.drop_duplicates().reset_index(drop=True)


main_df_visit = main_df_visit.drop(['time_difference','start_time_temp'],axis=1)
main_df_visit = main_df_visit.reset_index(drop=True)

cotraveler_df_visit_mask = cotraveler_df_visit.drop(['time_difference','start_time_temp'],axis=1)
cotraveler_df_visit_mask = cotraveler_df_visit_mask.reset_index(drop=True)


main_df_visit_filtered = pd.concat([main_df_visit_mask, main_df_visit], ignore_index=True).drop_duplicates(keep=False)
cotraveler_df_visit_filtered = pd.concat([cotraveler_df_visit_mask, cotraveler_df_visit], ignore_index=True).drop_duplicates(keep=False)


print(main_df_visit.shape)
print(main_df_visit_filtered.shape)

print(cotraveler_df_visit.shape)
print(cotraveler_df_visit_filtered.shape)


import folium
m = folium.Map(location=[main_df_visit['location_latitude'].mean(), main_df_visit['location_longitude'].mean()], zoom_start=13)

# Add markers for unique visits in main_df_visit
for index, row in main_df_visit_filtered.iterrows():
    folium.Circle(
        location=[row['location_latitude'], row['location_longitude']],
        radius=3,
        color='red',
        fill=True,
        fill_color='red',
        popup=f"Start: {row['start_time']}<br>End: {row['end_time']}",
        tooltip=f"Start: {row['start_time']}<br>End: {row['end_time']}"
    ).add_to(m)

# Add markers for unique visits in cotraveler_df_visit
for index, row in cotraveler_df_visit_filtered.iterrows():
    folium.Circle(
        location=[row['location_latitude'], row['location_longitude']],
        radius=5,
        color='blue',
        fill=True,
        fill_color='blue',
        popup=f"Start: {row['start_time']}<br>End: {row['end_time']}",
        tooltip=f"Start: {row['start_time']}<br>End: {row['end_time']}"
    ).add_to(m)

# Add markers for common visits
for index, row in df_merged.iterrows():
    folium.Circle(
        location=[row['latitude_grid'], row['longitude_grid']],
        radius=8,
        color='orange',
        fill=True,
        fill_color='orange',
        # popup=f"Start: {row['start_time']}<br>End: {row['end_time']}",
        # tooltip=f"Start: {row['start_time']}<br>End: {row['end_time']}"
    ).add_to(m)


m.show_in_browser()