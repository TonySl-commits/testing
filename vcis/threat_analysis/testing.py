from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.threat_analysis.threat_analysis_functions import ThreatAnalysisFunctions
import pandas as pd

tools = CassandraTools(verbose=True)
utils = CDR_Utils(verbose=True)
threatanalysis = ThreatAnalysisFunctions()

server = '10.1.2.205'
session = tools.get_cassandra_connection(server = server)
region = '142'
sub_region = '145'
table_id=714
local = False
distance = 50

device_id = '4e320097-5103-40eb-978e-fc079e1abba3'

start_date = '2022-01-01'
end_date = '2024-12-30'
start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(end_date)
start_date = int(start_date)
end_date = int(end_date)

sus_areas = ['Beirut','Baabda']
black_listed_devices = ['4e320097-5103-40eb-978e-fc079e1abba3','26e170d1-9bbc-4b1f-b3e9-4a7fd265a033']
max_time_gap = 8.64e+7 # 8.64e+7 is 1 day
date_event = '2024-02-15'
location_event = '33.85732523964605,35.493690040462354'


#########################################################

df_main = tools.get_device_history_geo(device_id, start_date, end_date, session = session,region=region,sub_region=sub_region)
df_blacklist_devices = tools.get_device_history_geo_chunks(black_listed_devices,session = session, start_date = start_date, end_date = end_date,region=region,sub_region=sub_region)

# df_history = pd.concat([pd.read_csv('C:/Users/zriyad/Desktop/VCIS/src/vcis/threat_analysis/Red.csv'), pd.read_csv('C:/Users/zriyad/Desktop/VCIS/src/vcis/threat_analysis/Green.csv')], ignore_index=True)
# df_blacklist_devices = pd.read_csv('C:/Users/zriyad/Desktop/VCIS/src/vcis/threat_analysis/Blue.csv')
df_threat = threatanalysis.get_threat_analysis_list(df_main, sus_areas, df_blacklist_devices, max_time_gap)
df_threat.to_csv('C:/Users/zriyad/Desktop/VCIS/src/vcis/threat_analysis/df_threat.csv')
print(df_threat)









