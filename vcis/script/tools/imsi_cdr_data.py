from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.script.nodes.azimuth_fix import ProcessBTSData
from vcis.utils.properties import CDR_Properties

import time
import warnings
warnings.filterwarnings('ignore')

##########################################################################################################

imsi_id = '151470885204890'
device_id = '0000000000000000'
check_device_id = ''

start_date = "2020-06-18"
end_date = "2024-01-18"
distance = 200

server = '10.10.10.101'
start_date = str(start_date)
end_date = str(end_date)

##########################################################################################################

cassandra_spark_tools = CassandraSparkTools()
bts_data_processor = ProcessBTSData()
utils = CDR_Utils(verbose = True)
properties = CDR_Properties()

start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(start_date)

local = True
distance =  500

# Read imsi data from cassandra
############################################################################################################

print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Fetch Device Data'))
df_device = cassandra_spark_tools.get_device_history_imsi_spark(imsi_id, start_date, end_date, server = '10.1.10.66', local=local)

print(df_device)
print(df_device.columns)

print(f'INFO:  Imsi Data imported from cassandra.')

# Read BTS Data from cassandra
##########################################################################################################

bts_data = bts_data_processor.read_results_from_cassandra(properties.cdr_bts_polygon_vertices_table_name)
print(bts_data)

print(f'INFO:  BTS Data imported from cassandra.')

# Save data into excel files
##########################################################################################################

df_device.to_excel('df_device_imsi.xlsx')
bts_data.to_excel('bts_data.xlsx')

print(f'INFO:  Data saved into excel files.')

##########################################################################################################
