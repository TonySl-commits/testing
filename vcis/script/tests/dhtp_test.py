
from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.plots.plots_tools import PlotsTools
from vcis.cotraveler.cotraveler_main import CotravelerMain
from vcis.reporting.report_main.report_generator import ReportGenerator
from vcis.ai.models.pandasai.pandasai_model import PandasAIModel
import pandas as pd

utils = CDR_Utils()
cassandra_tools = CassandraTools(verbose=True)
# latitude = 37.9244
# longitude = 41.692444

# latitude = 41.033636912199846
# longitude = 29.070250196961975

latitude = 33.88662476478706
longitude = 35.50906496300163
start_date = "2023-1-18"
end_date = "2024-5-18"
start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(end_date)
server = "10.1.2.110"
df_dhtp = cassandra_tools.get_device_dhtp(latitude=latitude,longitude=longitude,start_date=start_date,end_date=end_date,scan_distance = 300)
df_dhtp.to_csv('test2.csv')
# df_dhtp = pd.read_csv('test.csv')

df_dhtp = utils.convert_timestamp_to_date(df_dhtp)
df_dhtp['usage_timeframe'] = df_dhtp['usage_timeframe'].dt.date
print(df_dhtp['usage_timeframe'].unique())
