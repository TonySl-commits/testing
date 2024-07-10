from vcis.reporting.report_main.reporting_tools import ReportingTools
from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.reporting.report_main.report_generator import ReportGenerator
import plotly.graph_objs as go
import plotly.io as pio
import pandas as pd
from vcis.reporting.simulation_reporting.simulation_report_generator import SimulationReportGenerator

reporting_tools = ReportingTools()
utils = CDR_Utils(verbose=True)
properties = CDR_Properties()
oracle_tools = OracleTools()
report = ReportGenerator()   
simulation_report_generator = SimulationReportGenerator()
table_id = 183119
blocks_dictionary = {}
blocks_aoi_dictionary = {}
query = f"SELECT * FROM locdba.loc_report_config where LOC_REPORT_CONFIG_ID ={table_id} "

result= oracle_tools.get_oracle_query(query)

report_type = result['LOC_REPORT_TYPE'][0]
if report_type==11:
    oracle_table = oracle_tools.get_table("SSDX_ENG","DATACROWD_CDR_DATA",table_id,True)
else:    
    oracle_table = oracle_tools.get_table("SSDX_ENG","TMP_REPORT_COORDINATES_6",table_id,True)


query = f"SELECT REPORT_TYPE , REPORT_NAME FROM TECHDBA.CYB_REPORT_HTML_TEMPLATES where REPORT_TYPE={report_type}"
table_type = oracle_tools.get_oracle_query(query)
report_name = table_type['REPORT_NAME'][0]


table = reporting_tools.get_report_blocks_table(table_id=table_id)
# print(table)
number_of_hits_per_service_provider = table[table['BLOCK_NAME']=='Number of Hits per Service Provider']['BLOCK_CONTENT'].values[0]
number_of_hits_per_service_provider = pio.from_json(number_of_hits_per_service_provider, output_type='Figure')
blocks_dictionary['number_of_hits_server_providers'] = number_of_hits_per_service_provider

number_of_hits_per_dow = table[table['BLOCK_NAME']=='Number of Hits per Day of Week']['BLOCK_CONTENT'].values[0]
number_of_hits_per_dow =  pio.from_json(number_of_hits_per_dow, output_type='Figure')
blocks_dictionary['number_of_hits_per_dow'] = number_of_hits_per_dow

number_of_hits_per_month = table[table['BLOCK_NAME']=='Number of Hits per Month']['BLOCK_CONTENT'].values[0]
number_of_hits_per_month =  pio.from_json(number_of_hits_per_month, output_type='Figure')
blocks_dictionary['number_of_hits_per_month'] = number_of_hits_per_month

number_of_hits_per_hod = table[table['BLOCK_NAME']=='Number of Hits per Hour of Day']['BLOCK_CONTENT'].values[0]
number_of_hits_per_hod =  pio.from_json(number_of_hits_per_hod, output_type='Figure')
blocks_dictionary['number_of_hits_per_hod'] = number_of_hits_per_hod

summary_statistics = table[table['BLOCK_NAME']=='Summary Statistics']['BLOCK_CONTENT'].values[0]
summary_statistics =  pd.read_json(summary_statistics, orient='records')
blocks_dictionary['summary_statistics'] = summary_statistics
# print(summary_statistics)

timeline = table[table['BLOCK_NAME']=='Timeline']['BLOCK_CONTENT'].values[0]
blocks_dictionary['timeline'] = timeline
# df = pd.DataFrame(summary_statistics)
main_map = simulation_report_generator.get_main_map(oracle_table,table_id)
blocks_dictionary["main_map"] = main_map

if len(table[table['BLOCK_ASSOCIATION']=='aoi']!=0):
    unique_device = table[table['DEVICE_ID']!='null']['DEVICE_ID'].unique().tolist()

    for device_id in unique_device:
        aoi_device_blocks_dictionary = {}
        df_device = table[table['DEVICE_ID']==device_id]

        # Confidence Metrics
        gauge_chart = df_device[df_device['BLOCK_NAME']=='Gauge Chart']['BLOCK_CONTENT'].values[0]
        gauge_chart =  pio.from_json(gauge_chart, output_type='Figure')
        confidence_metrics = df_device[df_device['BLOCK_NAME']=='Confidence Metrics']['BLOCK_CONTENT'].values[0]
        confidence_metrics = pio.from_json(confidence_metrics, output_type='Figure')
        confidence_score_explanation = df_device[df_device['BLOCK_NAME']=='Confidence Score Description']['BLOCK_CONTENT'].values[0]
        metrics_explanation = df_device[df_device['BLOCK_NAME']=='Confidence Metrics Explanation']['BLOCK_CONTENT'].values[0]
        aoi_device_blocks_dictionary['gauge'] = gauge_chart
        aoi_device_blocks_dictionary['confidence_metrics'] = confidence_metrics
        aoi_device_blocks_dictionary['confidence_score_explanation'] = confidence_score_explanation
        aoi_device_blocks_dictionary['metrics_explanation'] = metrics_explanation

        # AOI Metrics
        hits_per_dow = df_device[df_device['BLOCK_NAME']=='Number of Hits Per Day of Week']['BLOCK_CONTENT'].values[0]
        hits_per_dow =  pio.from_json(hits_per_dow, output_type='Figure')
        dow_description = df_device[df_device['BLOCK_NAME']=='Day of Week Report Description']['BLOCK_CONTENT'].values[0]

        hits_per_month = df_device[df_device['BLOCK_NAME']=='Number of Hits Per Month']['BLOCK_CONTENT'].values[0]
        hits_per_month =  pio.from_json(hits_per_month, output_type='Figure')
        month_description = df_device[df_device['BLOCK_NAME']=='Month Report Description']['BLOCK_CONTENT'].values[0]

        likelihood_dow = df_device[df_device['BLOCK_NAME']=='Location Likelihood Per Day of Week']['BLOCK_CONTENT'].values[0]
        likelihood_dow =  pio.from_json(likelihood_dow, output_type='Figure')
        likelihood_dow_description = df_device[df_device['BLOCK_NAME']=='Likelihood Description']['BLOCK_CONTENT'].values[0]

        duration_dow = df_device[df_device['BLOCK_NAME']=='Location Duration Per Day of Week']['BLOCK_CONTENT'].values[0]
        duration_dow =  pio.from_json(duration_dow, output_type='Figure')
        duration_dow_description = df_device[df_device['BLOCK_NAME']=='Duration Description']['BLOCK_CONTENT'].values[0]

        aoi_device_blocks_dictionary['hits_per_dow'] = hits_per_dow
        aoi_device_blocks_dictionary['hits_per_dow_description'] = dow_description
        aoi_device_blocks_dictionary['hits_per_month'] = hits_per_month
        aoi_device_blocks_dictionary['hits_per_month_description'] = month_description
        aoi_device_blocks_dictionary['lilkelihood_dow'] = likelihood_dow
        aoi_device_blocks_dictionary['lilkelihood_dow_description'] = likelihood_dow_description
        aoi_device_blocks_dictionary['duration_dow'] = duration_dow
        aoi_device_blocks_dictionary['duration_dow_description'] = duration_dow_description

        blocks_aoi_dictionary[device_id] = aoi_device_blocks_dictionary



# print(blocks_aoi_dictionary)
report,report_intoduction = reporting_tools.get_simulation_report_layout(main_table = result,report_table = oracle_table,report_name=report_name,report_type=report_type ,blocks=blocks_dictionary,table_id=table_id, blocks_aoi_dictionary=blocks_aoi_dictionary)

reporting_tools.save_report(report = report ,report_path = properties.passed_filepath_reports_html,open=True,report_name=properties.passed_report_name_html_simulation)



