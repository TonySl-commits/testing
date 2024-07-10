import time
import datetime
import json

from vcis.utils.utils import CDR_Utils , CDR_Properties
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.event_tracing.common_devices_functions import CommonDevicesFunctions

start_time_test = time.time()
properties = CDR_Properties()
data = {
    'case_id': '186008',
    'start_date': '2021-01-01',
    'server': '10.1.10.110',
    'region': 142,
    'sub_region': 145,
    'black_listed_devices': ['95cca195-2b5e-4a9b-ab9f-48995eeb5d1f'],
    'sus_areas': ['Beirut'],
    'max_time_gap': 10000
}
black_listed_devices = None
sus_areas = None
max_time_gap = None

if data['black_listed_devices'] == []:
    #do something
    print('hi')
else:
    black_listed_devices = data['black_listed_devices']
if data['sus_areas'] == []:
    #do something
    print('hi')
else:
    sus_areas = data['sus_areas']

if data['max_time_gap'] == None:
    max_time_gap = 10000
else:
    max_time_gap = data['max_time_gap']

start_time_test = time.time()
case_id=data['case_id']
start_date=data['start_date']
end_date= datetime.datetime.now().strftime("%Y-%m-%d")
server=data['server']
try:
    region = data['region']
    sub_region = data['sub_region']
except:
    region=142
    sub_region=145

##########################################################################################################

utils = CDR_Utils(verbose=True)
oracle_tools = OracleTools(verbose=True)
cassandra_tools = CassandraTools()
common_devices_functions = CommonDevicesFunctions()
##########################################################################################################

local = True
distance = 100

start_date = str(start_date)
end_date = str(end_date)

start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(end_date)
session = cassandra_tools.get_cassandra_connection(server)
step_lat,step_lon = utils.get_step_size(distance)
# ##########################################################################################################
# try:
table = oracle_tools.get_table("SSDX_ENG","TMP_REPORT_COORDINATES_6",case_id,True)
d_list = table['DEVICE_ID'].unique().tolist()

df_history = cassandra_tools.get_device_history_geo_chunks(d_list,start_date=start_date,end_date=end_date,session=session,region=region,sub_region=sub_region)
device_id_mapping = {old_id: f"Device_{str(i+1).zfill(3)}" for i, old_id in enumerate(df_history['device_id'].unique())}
df_history['device_id_s'] = df_history['device_id'].map(device_id_mapping)
df_blacklist_devices = cassandra_tools.get_device_history_geo_chunks(black_listed_devices,session = session, start_date = start_date, end_date = end_date,region=region,sub_region=sub_region)


df_history = utils.binning(df_history,100)
df_history = utils.combine_coordinates(df_history)

# read df_history
# df_history = pd.read_csv('df_history.csv')
df_common = common_devices_functions.merge_per_device(df_history,table,step_lat = step_lat,step_lon = step_lon)
df_common = utils.add_reverseGeocode_columns(df_common)
df_common['device_id_s'] = df_common['device_id'].map(device_id_mapping)
df_common = utils.binning(df_common,100)
df_common = utils.combine_coordinates(df_common)
df_common = df_common.drop_duplicates()
# df_common.to_csv('df_common.csv')
# df_common = pd.read_csv('df_common.csv')

groups,df_common = common_devices_functions.get_groups(df_common)
# groups['group_id'] = range(len(groups))
groups['group_ids'] = groups['group_ids'].astype(str)
import ast
print(len(ast.literal_eval(groups['group_ids'].iloc[0])))
print(duh)
df_history.to_csv('df_history_008.csv',index=False)
df_common.to_csv('df_common_008.csv',index=False)
groups.to_csv('groups_008.csv',index=False)

# df_history = pd.read_csv('df_history_913.csv')
df_history = utils.add_reverseGeocode_columns(df_history)
# df_common = pd.read_csv('df_common_913.csv')
df_common = utils.add_reverseGeocode_columns(df_common)
# groups = pd.read_csv('groups_913.csv')
devices_threat_table = common_devices_functions.get_threat_table_devices(df_history,groups,df_blacklist_devices, max_time_gap,distance=distance)
df2_exploded = devices_threat_table.explode('Contaced Black Listed Device(s)')

import random
one_day_ms = 24 * 60 * 60 * 1000
thirty_days_ms = 30 * one_day_ms

# Adjust USAGE_TIMEFRAME by adding or subtracting a random number of milliseconds within the specified range
groups['usage_timeframe'] = groups['usage_timeframe'].apply(lambda x: str(int(x) + random.randint(-thirty_days_ms, thirty_days_ms)))
groups['usage_timeframe'] = groups['usage_timeframe'].astype(str)

groups = common_devices_functions.get_graphs_for_groups(df_history, df_common=df_common,groups=groups,session=session,start_date=start_date,end_date=end_date,region=region,sub_region=sub_region)

stat_table = common_devices_functions.get_stat_table(df_common)
stat_table_html = utils.add_css_to_html_table(stat_table)
# groups['HTML_GRAPHS'] = groups['HTML_GRAPHS'].apply(lambda x: base64.b64encode(x.encode('utf-8')).decode('utf-8'))

df_common = df_common[['device_id', 'location_latitude','location_longitude', 'usage_timeframe', 'main_id','service_provider_id', 'location_name']]
print(groups.columns)
oracle_tools.drop_create_insert(df_common,properties.common_devices_table ,case_id,properties._oracle_table_schema_query_common_devices)
oracle_tools.drop_create_insert(groups,properties.common_devices_group_table ,case_id,properties._oracle_table_schema_query_common_devices_group)




# except Exception as e :
#     print(e)
#     table = None
#     print('Error in common_devices_api')

#     return '-1'
##########################################################################################################

end_time_test = time.time()

print("Time taken: ", (end_time_test - start_time_test) / 60, "minutes")










# import pandas as pd
# df1_exploded = groups.explode('group_ids').rename(columns={'group_ids': 'device_id'})
# df2_exploded = devices_threat_table.explode('Contaced Black Listed Device(s)')

# print("df1",df1_exploded)
# print("df2",df2_exploded)

# # Merge df1 and df2_exploded on 'device_id'
# merged_df = df1_exploded.merge(df2_exploded, on='device_id', how='left')
# print(merged_df)

# # Check if any device in each group has contacted a blacklisted device
# group_contact_blacklist = merged_df.groupby('group_id')['Contaced Black Listed Device(s)'].apply(lambda x: x.notna().any()).reset_index()
# print(group_contact_blacklist)
# # Rename the column for clarity
# group_contact_blacklist.rename(columns={'Contaced Black Listed Device(s)': 'contacted_blacklisted'}, inplace=True)
# print(group_contact_blacklist)
# # Replace True/False with 'Yes'/'No'
# group_contact_blacklist['contacted_blacklisted'] = group_contact_blacklist['contacted_blacklisted'].map({True: 'Yes', False: 'No'})
# print(group_contact_blacklist)