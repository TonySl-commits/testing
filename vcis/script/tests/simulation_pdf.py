from vcis.cotraveler.cotraveler_functions import CotravelerFunctions
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.reporting.report_main.report_generator import ReportGenerator
from vcis.utils.utils import CDR_Utils
import country_converter as coco
import pandas as pd
from datetime import timedelta

utils = CDR_Utils(verbose=True)
oracle_tools = OracleTools()
report = ReportGenerator()   
cotraveler_function = CotravelerFunctions()


table_id = 	151143


query = f"SELECT * FROM locdba.loc_report_config where LOC_REPORT_CONFIG_ID ={table_id} "

result= oracle_tools.get_oracle_query(query)

print(result.columns)
report_type = result['LOC_REPORT_TYPE'][0]
if report_type==11:
    table = oracle_tools.get_table("SSDX_ENG","DATACROWD_CDR_DATA",table_id,True)
else:    
    table = oracle_tools.get_table("SSDX_ENG","TMP_REPORT_COORDINATES_6",table_id,True)

print(table)
print(table.columns)
query = f"SELECT REPORT_TYPE , REPORT_NAME FROM TECHDBA.CYB_REPORT_HTML_TEMPLATES where REPORT_TYPE={report_type}"
table_type = oracle_tools.get_oracle_query(query)
report_name = table_type['REPORT_NAME'][0]
print(report_name)
table.columns = table.columns.str.lower()

table = utils.add_reverseGeocode_columns(table)

table = table[ ['location_name','device_id','usage_timeframe', 'location_latitude','location_longitude','city','country']]
print(table)

cities_list = table['city'].unique().tolist()
counrties_list = table['country'].unique().tolist()
device_id_list = table['device_id'].unique().tolist()
counrties_list = coco.convert(names=counrties_list, to='name_short')
print(cities_list,counrties_list)

table['usage_timeframe'] = pd.to_datetime(table['usage_timeframe'], unit='ms')

table = utils.binning(table)

print(table)
df = table.copy()
# # Sort dataframe by 'usage_timeframe'
# df = df.sort_values(by='usage_timeframe')

# # Group by 'latitude_grid' and 'longitude_grid'
# grouped = df.groupby(['latitude_grid', 'longitude_grid'])
# # Define a function to check if two timestamps are within the threshold
# def within_threshold(ts1, ts2, threshold):
#     return abs((ts1 - ts2).total_seconds()) <= threshold.total_seconds()


# # Define the timestamp threshold (5 minutes)
# threshold = timedelta(minutes=5)

# # Initialize a dictionary to store the results
# results = {}

# for group_name, group_df in grouped:
#     # Initialize a set to store device IDs for this group
#     device_ids = set()
#     # Initialize a list to store timestamp ranges
#     timestamp_ranges = []
    
#     # Iterate over rows in the group dataframe
#     for idx, row in group_df.iterrows():
#         # Check if this device ID is already in the set for this group
#         if row['device_id'] not in device_ids:
#             # Add this device ID to the set
#             device_ids.add(row['device_id'])
            
#             # Find other device IDs within the timestamp threshold
#             close_devices = group_df[(group_df['usage_timeframe'] != row['usage_timeframe']) & 
#                                       (group_df['device_id'] != row['device_id']) & 
#                                       (group_df['usage_timeframe'].apply(lambda x: within_threshold(x, row['usage_timeframe'], threshold)))]
            
#             # Add the other device IDs to the set
#             device_ids.update(close_devices['device_id'])
            
#             if len(close_devices) > 0:
#                 min_ts = min(close_devices['usage_timeframe'].min(), row['usage_timeframe'])
#                 max_ts = max(close_devices['usage_timeframe'].max(), row['usage_timeframe'])
#                 timestamp_ranges.append((min_ts, max_ts))
    
#     # Store the device IDs and timestamp ranges for this group in the results dictionary
#     results[group_name] = {'device_ids': list(device_ids), 'timestamp_ranges': timestamp_ranges}

# # Display the results
# # for key, value in results.items():
# #     if len(value['device_ids'])>=2 & len(value['timestamp_ranges']) ==0:
# #         print(f"Location: {key}, Device IDs: {value['device_ids']}, Timestamp Ranges: {value['timestamp_ranges']}")

# print(results)

from geopy.distance import geodesic

# Distance threshold in kilometers
# Parameters
distance_threshold = 1  # Your distance threshold in km
time_threshold = timedelta(minutes=5)  # Your time threshold in minutes

# Create a copy of the DataFrame to store the results
result_df = df.copy()

# Initialize a new column to store the group ID for each device
result_df['group_id'] = None

# Iterate over each row
for idx, row in df.iterrows():
    # Calculate the distance to all other points
    distances = df.apply(lambda x: geodesic((row['location_latitude'], row['location_longitude']),
                                             (x['location_latitude'], x['location_longitude'])).kilometers,
                          axis=1)
    
    # Find points within the distance threshold and under the time threshold
    close_points = df[(distances <= distance_threshold) &
                      (abs(df['usage_timeframe'] - row['usage_timeframe']) <= time_threshold)]
    
    # Assign group ID to close points
    result_df.loc[close_points.index, 'group_id'] = idx

# Drop rows without group ID (i.e., points not in any group)
result_df.dropna(subset=['group_id'], inplace=True)

# Print the result
print(result_df)