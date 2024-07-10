from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.utils.utils import CDR_Utils
from vcis.threat_analysis.threat_analysis_functions import ThreatAnalysisFunctions

tools = CassandraTools()
utils = CDR_Utils()
threatanalysis = ThreatAnalysisFunctions()

server = ''
session = tools.get_cassandra_connection()
region = 142
sub_region = 145
table_id=714
local = False
distance = 50

device_id = ''

start_date = ''
end_date = ''
start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(end_date)
start_date = int(start_date)
end_date = int(end_date)

sus_areas = ['','','']
black_listed_devices = ['','']

df_device = tools.get_device_history_geo(device_id, start_date, end_date,server=server, session = session)
df_blacklist_devices = tools.get_device_history_geo_chunks(black_listed_devices,session = session, start_date = start_date, end_date = end_date,region=region,sub_region=sub_region)


