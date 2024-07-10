from vcis.utils.utils import CDR_Utils, CDR_Properties

import time
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.cotraveler.cotraveler_main import CotravelerMain
from vcis.reporting.report_main.report_generator import ReportGenerator

##########################################################################################################

utils = CDR_Utils(verbose=True)
properties = CDR_Properties()
oracle_tools = OracleTools()
cotraveler_main = CotravelerMain(verbose=True)
geo_report = ReportGenerator(verbose=True)

##########################################################################################################


# CC0B4054-CDD5-4162-9165-237E66696C98
# 3355f290-11b9-41ab-aaad-3d7668007e98
device_id = 'e25d599e-c5c8-4e40-a810-880aaee58b88'
local = False
distance = 50

start_date = "2022-01-01"
end_date = "2024-01-30"
server = '10.1.2.205'

region = 142
sub_region = 145
table_id=714
##########################################################################################################

start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(end_date)

start_date = int(start_date)
end_date = int(end_date)
distance = 50
########################################################################################################## 

start_time_test = time.time()
# # table = pd.read_csv('table.csv')


table , df_merged_devices ,df_device ,df_common,df_history, distance,heatmap_plots,cotraveler_barchart,cotraveler_user_prompt = cotraveler_main.cotraveler(device_id=device_id,
                start_date=start_date,
                end_date=end_date,
                local=local,
                distance=distance,
                server=server,
                region=region,
                sub_region = sub_region)

df_merged_devices.to_csv(properties.passed_filepath_excel + 'df_merged_devices.csv',index=False)
df_history.to_csv(properties.passed_filepath_excel + 'df_history.csv',index=False)
df_device.to_csv(properties.passed_filepath_excel + 'df_main.csv',index=False)
df_common.to_csv(properties.passed_filepath_excel + 'df_common.csv',index=False)
table.to_csv(properties.passed_filepath_excel + 'table.csv',index=False)

# table= pd.read_csv(properties.passed_filepath_excel+'table.csv')
# table['COMMON_LOCATIONS_HITS'] = table['COMMON_LOCATIONS_HITS'].astype(str)

# table_insert = table[['RANK','DEVICE_ID','GRID_SIMILARITY','COUNT','COMMON_LOCATIONS_HITS','SEQUENCE_DISTANCE','LONGEST_SEQUENCE']]
# oracle_spark_tools.create_temp_table(table,properties.co_traveler_table ,table_id , clob_columns_name = 'COMMON_LOCATIONS_HITS',server=server)

# oracle_tools.drop_create_insert(table_insert,
#                                 properties.co_traveler_table,
#                                 table_id,
#                                 properties._oracle_table_schema_query_cotraveler
#                                 )

# end_time_test = time.time()
# print("Time taken: ", (end_time_test - start_time_test) / 60, "minutes")

##########################################################################################################
# df_merged_list = pd.read_csv(properties.passed_filepath_excel+'df_merged_devices.csv')
# df_device = pd.read_csv(properties.passed_filepath_excel+'df_device.csv')
# df_history = pd.read_csv(properties.passed_filepath_excel+'df_history.csv')
# df_common = pd.read_csv(properties.passed_filepath_excel+'df_common.csv')
# df_merged_list = pd.read_csv(properties.passed_filepath_excel+'df_merged_list.csv')


# print(df_device['usage_timeframe'].head(1))
# print(df_history['usage_timeframe'].head(1))
# print(df_common['usage_timeframe'].head(1))


# df_device = utils.convert_ms_to_datetime(df_device,timestamp_column='usage_timeframe')
# df_history = utils.convert_ms_to_datetime(df_history,timestamp_column='usage_timeframe')

# df_device = df_device.drop_duplicates(subset=['device_id','location_latitude','location_longitude','usage_timeframe'])
# df_history = df_history.drop_duplicates(subset=['device_id','location_latitude','location_longitude','usage_timeframe'])
# df_common = df_common.drop_duplicates(subset=['device_id','location_latitude','location_longitude','usage_timeframe'])

# test = df_history[df_history['device_id'] == '3355f290-11b9-41ab-aaad-3d7668007e98']
# test2 = df_common[df_common['device_id'] == '3355f290-11b9-41ab-aaad-3d7668007e98']

# print(test.shape)
# print(test2.shape)
# print(df_device.shape)
# geo_report.get_geo_report(dataframe = df_merged_list,df_main = df_device,df_common = df_common,table = table,df_history = df_history,report_type='cotraveler',file_name='cotraveler_report',table_id=table_id,distance = distance)
