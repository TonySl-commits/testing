from vcis.cotraveler.cotraveler_functions import CotravelerFunctions
from vcis.utils.properties_ai import AIProperties
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.reporting_pdf.simulation_reporting.simulation_report_functions import SimulationReportFunctions
from vcis.reporting_pdf.simulation_reporting.simulation_report_layout import SimulationReportLayout
from vcis.reporting_pdf.simulation_reporting.simulation_report_layout_html import SimulationReportLayoutHTML
from vcis.ai.models.groq.model import GroqAIModel
from vcis.reporting_pdf.report_main_pdf.pdf_reporting_tools import ReportingTools
from vcis.reporting.report_main.reporting_tools import ReportingTools as ReportingToolsHTML
from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.ai.tools.vcis_tools import vcisTools
from vcis.databases.oracle.oracle_tools import OracleTools

import pandas as pd

cassandra_tools = CassandraTools(verbose = True)
properties = CDR_Properties()
utils = CDR_Utils(verbose=True)
oracle_tools = OracleTools()
cotravelers_functions = CotravelerFunctions()
table_id = '179401'
distance = 100
query = f"SELECT * FROM locdba.loc_report_config where LOC_REPORT_CONFIG_ID ={table_id} "

result= oracle_tools.get_oracle_query(query)

print(result.columns)
report_type = result['LOC_REPORT_TYPE'][0]
if report_type==11:
    table = oracle_tools.get_table("SSDX_ENG","DATACROWD_CDR_DATA",table_id,True)
else:    
    table = oracle_tools.get_table("SSDX_ENG","TMP_REPORT_COORDINATES_6",table_id,True)
start_date = result['FILTER_BDATE'][0].strftime("%Y-%m-%d")
end_date = result['FILTER_EDATE'][0].strftime("%Y-%m-%d")
print(start_date)
print(end_date)

print(table)
print(table.columns)
query = f"SELECT REPORT_TYPE , REPORT_NAME FROM TECHDBA.CYB_REPORT_HTML_TEMPLATES where REPORT_TYPE={report_type}"
table_type = oracle_tools.get_oracle_query(query)
report_name = table_type['REPORT_NAME'][0]
print(report_name)

start_date = utils.convert_datetime_to_ms(str(start_date))
end_date = utils.convert_datetime_to_ms(str(end_date))
print(start_date)
print(end_date)

devices_list = table['DEVICE_ID'].unique().tolist()
print(f'Number of devices {len(devices_list)}')
region = 142
sub_region = 145
server = '10.1.10.110'
# # device_list_separated = cotravelers_functions.separate_list(devices_list,15)
# session = cassandra_tools.get_cassandra_connection(server=server)
# df_history = cassandra_tools.get_device_history_geo_chunks(session = session, device_list_separated = devices_list, start_date= start_date, end_date = end_date,region = region , sub_region = sub_region , server = server)
# df_history.to_csv(properties.passed_filepath_excel + 'simulation_history.csv')
df_history = pd.read_csv(properties.passed_filepath_excel + 'simulation_history.csv')

df_history = utils.binning(df_history,distance)

df_history = utils.combine_coordinates(df_history)
print(df_history)

# df_grouped = df_history.groupby('grid').agg({"device_id" : list, "usage_timeframe" : list})
df_grouped = df_history.groupby('grid')['device_id'].unique().reset_index()
df_grouped['device_list_length'] = df_grouped['device_id'].apply(len)
df_grouped = df_grouped.sort_values(by='device_list_length', ascending=False)
print(df_grouped)
