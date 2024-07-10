from vcis.databases.cassandra.cassandra_tools import CassandraTools
import pandas as pd
from vcis.utils.utils import CDR_Properties,CDR_Utils

cassandra_tools = CassandraTools(verbose = True)
properties = CDR_Properties()
utils = CDR_Utils(verbose=True)

start_date = "2024-05-01"
end_date = "2024-05-30"
server = "10.1.2.205"

start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(end_date)

start_date = int(start_date)
end_date = int(end_date)

year = "2024"
month = "5"
region = "142"
sub = "145"

query = """SELECT DISTINCT device_id FROM datacrowd.geo_data_year_month_region_subre"""

query = query.replace('year', year) \
    .replace('month', month) \
    .replace('region', region) \
    .replace('subre', sub)

session = cassandra_tools.get_cassandra_connection(server=server)
devices_list = [row.device_id for row in session.execute(query)]
print(devices_list)
df_history = cassandra_tools.get_device_history_geo_chunks(devices_list,start_date=start_date,end_date=end_date,region = region, sub_region= sub,session=session)

df_history = utils.binning(df_history,distance = 100)
print(df_history)


