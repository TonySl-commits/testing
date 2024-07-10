# IMPORT REQUIRED LIBRARIES AND CREATE INSTANCES OF THE PACKAGES
import pandas as pd
from vcis.utils.utils import CDR_Utils
from vcis.coexistence.coexistence_functions import CoexistenceFunctions
from vcis.databases.cassandra.cassandra_tools import CassandraTools

coexistence_functions = CoexistenceFunctions()
utils = CDR_Utils()
tools = CassandraTools()

distance = 200
# server = "10.1.10.110"
server = "10.1.2.205"
# SET THE MAIN DEVICE ID ALONG WITH THE DATE OF ITS EXISTENCE
# device_id = 'b5497f50-23e2-4be2-9731-ea1b42e21456' 

# device_id = 'f42568c1-5d40-46ac-b2ed-b3c9544db5c5'
# start_date = "2023-01-01"    
# end_date = "2023-03-30"

device_id = "a8cc39bf-435e-4d8d-ac25-3bc2051ef99c"
# device_id = '53b5912e-52f5-42d2-8cad-f61de44fc216' 
start_date = "2022-01-01"    
end_date = "2024-12-30"
start_date = str(start_date)
end_date = str(end_date)
start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(end_date)

# GET THE HISTORY OF THE MAIN DEVICE AND PERFORM THE NEEDED PROCESSING ON IT
df_device = tools.get_device_history_geo(device_id, start_date, end_date,server=server)
df_device = utils.binning(df_device,distance)
df_device = utils.combine_coordinates(df_device)
df_device = utils.convert_ms_to_datetime(df_device)
# df_device.to_csv('df_device.csv')
# df_device = pd.read_csv('df_device.csv')
print('HISTORY OF THE MAIN DEVICE: \n', df_device)

# GET THE DEVICES THAT SHARE THE SAME GRIDS WITH THE MAIN DEVICE
df_common = coexistence_functions.get_common_df(df_device,passed_server = server, scan_distance=distance, default_fetch_size=50000)
device_list = df_common['device_id'].unique().tolist()
print("There are",len(device_list),"unique devices found while scanning the grids.")
print('Our made up coexistor is in the unqiue devices:','53b5912e-52f5-42d2-8cad-f61de44fc216' in device_list) 
device_list = coexistence_functions.separate_list(device_list,10)
print("There are",len(device_list),"lists.")
# df_common.to_csv('df_common.csv')
# df_common = pd.read_csv('df_common.csv')
print('HISTORY OF THE ALL THE DEVICES: \n', df_common)

# GET THE HISTORY OF EACH DEVICE FOUND AND PERFORM THE NEEDED PROCESSING ON THEM
time_begin, time_end = coexistence_functions.calculate_timeframe(df_device)
df_history = tools.get_device_history_geo_chunks(device_list, time_begin, time_end,server)
print('53b5912e-52f5-42d2-8cad-f61de44fc216' in df_history["device_id"].tolist())
df_history = coexistence_functions.exclude_existing_devices(df_history, time_begin)
print('53b5912e-52f5-42d2-8cad-f61de44fc216' in df_history["device_id"].tolist())
df_history = utils.binning(df_history,distance)
df_history = utils.combine_coordinates(df_history)
df_history.to_csv('df_history.csv')
# df_history = pd.read_csv('df_history.csv')
# print('HISTORY OF THE ALL THE DEVICES: \n', df_history)

# FILTER THE HISTORY DATAFRAME
# df_history = coexistence_functions.filter(df_device,df_history)
# print('Filtered History DataFrame:\n',df_history)

# GET THE DECISION MAKING METRICS
df_metrics = coexistence_functions.metrics(df_history, df_common, df_device)
print('Metrics DataFrame:\n',df_metrics)
# df_metrics.to_csv("df_metrics.csv")


# Zabet el final decision metric
# zabet el distance metric
# Zabet el area metric
# Work on metric of time
# Zabet el df metric