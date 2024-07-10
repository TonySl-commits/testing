from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.correlation.cdr_correlation_functions import CDRCorrelationFunctions
from vcis.plots.plots_tools import PlotsTools
from vcis.databases.cassandra.cassandra_tools import CassandraTools

##########################################################################################################

properties = CDR_Properties()
utils = CDR_Utils()
plots_tools = PlotsTools()
cassandra_tools = CassandraTools()
##########################################################################################################

geo_id = '4ba0c8c3-e6c2-4e3a-89f0-18eb148bae59'
geo_id_cotraveler = '47bc4a71-6d1a-41a6-aba6-2a0c8dd23757'
start_date = '2023-06-18'
end_date = '2024-01-18'
server = '10.10.10.101'

correlation_functions = CDRCorrelationFunctions()
utils = CDR_Utils(verbose=True)

start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(end_date)
##########################################################################################################

df_geo = cassandra_tools.get_device_history_geo(device_id= geo_id ,start_date= start_date ,end_date= end_date,server=server)

df_geo_cotraveler = cassandra_tools.get_device_history_geo(device_id= geo_id_cotraveler ,start_date= start_date ,end_date= end_date,server = server)

##########################################################################################################

df_geo = utils.convert_ms_to_datetime(df_geo )
df_geo_cotraveler = utils.convert_ms_to_datetime(df_geo_cotraveler)

##########################################################################################################

df_geo.to_csv(properties.passed_filepath_excel + 'df_geo.csv')
df_geo_cotraveler.to_csv(properties.passed_filepath_excel + 'df_geo_cotraveler.csv')
