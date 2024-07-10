import pandas as pd
from vcis.utils.utils import CDR_Utils, CDR_Properties
from vcis.cotraveler.cotraveler_functions import CotravelerFunctions
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.plots.cotraveler_plots import CotravelerPlots

##########################################################################################################

# class CotravelerMain():
#     def __init__(self, verbose: bool = False):
#         self.cotraveler_functions = CotravelerFunctions()
#         self.utils = CDR_Utils(verbose)
#         self.properties = CDR_Properties()
#         self.cassandra_tools = CassandraTools(verbose)
#         self.cassandra_spark_tools = CassandraSparkTools()
#         self.cotraveler_plots = CotravelerPlots(verbose)

#         self.verbose = verbose

#     def cotraveler(self,
#                 device_id:str,
#                 start_date:int,
#                 end_date:int,
#                 local:bool = True,
#                 region:int = 142,
#                 sub_region:int = 145,
#                 distance:int = 50,
#                 server:str = '10.10.10.110'
#                 ):
#         print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Fetch Device Data'.center(50)))
cassandra_tool = CassandraTools(True)
utils = CDR_Utils()
session = cassandra_tool.get_cassandra_connection(server='10.1.10.110')
start_date = utils.convert_datetime_to_ms_str('2023-01-01')
end_date = utils.convert_datetime_to_ms_str('2025-01-01')
default_fetch_size = 30000
print(start_date)
df = cassandra_tool.get_device_history_imsi("151967466595180",start_date=start_date, end_date=end_date,session=session,default_fetch_size=default_fetch_size)
df.to_csv(r"device_history_imsi_30000rows.csv")

print(df)


