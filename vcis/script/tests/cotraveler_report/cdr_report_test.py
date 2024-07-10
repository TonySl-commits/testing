from vcis.reporting.report_main.report_generator import ReportGenerator
from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.correlation.correlation_main import CorrelationMain
from vcis.databases.cassandra.cassandra_tools import CassandraTools

data = {"end_date":"2024-02-04","server":"10.1.10.110","id":"121415223435890","table_id":155711,"start_date":"2023-03-11"}

table_id=data['table_id']
imsi_id=data['id']
start_date=data['start_date']
end_date=data['end_date']
server=data['server']

utils = CDR_Utils(verbose=True)
correlation_main = CorrelationMain()
report = ReportGenerator()
cassandra_tools = CassandraTools()

start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(end_date)

df_device = cassandra_tools.get_device_history_imsi(imsi_id, start_date, end_date ,server = server)
print(df_device)
report.get_cdr_report(dataframe = df_device,file_name = 'cdr_report',table_id = table_id)