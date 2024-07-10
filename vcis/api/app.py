import uvicorn
from fastapi import FastAPI, Body
import time
import json
import pandas as pd
import warnings
warnings.filterwarnings('ignore')


from vcis.utils.utils import CDR_Utils , CDR_Properties
from vcis.aoi_classification.geolocation_data_analyzer import GeoLocationAnalyzer
from vcis.cotraveler.cotraveler_functions import CotravelerFunctions
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.aoi_classification.aoi_correlation import AOI_Correlation
from vcis.confidence_analysis.geo_confidence_analysis import GeospatialConfidenceAnalysis
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.reporting.report_main.report_generator import ReportGenerator
# from vcis.ai.models.starcoder.starcoder_model import StarCoderAgent
from vcis.timeline.timeline_functions import TimeLineFunctions
from vcis.correlation.correlation_main import CorrelationMain
from vcis.cotraveler.cotraveler_main import CotravelerMain


##########################################################################################################
global properties
properties = CDR_Properties()

app = FastAPI()

# @app.post("/aoi")

# async def read_root(string_entity: str = Body(...)):

#     # Get the api data
#     api_data = json.loads(string_entity)

#     device_ids = api_data['device_id']
#     table_id = api_data['table_id']
#     server = api_data['server']
#     start_date = api_data['start_date']
#     end_date = api_data['end_date']
#     try:
#         region = api_data['region']
#         sub_region = api_data['sub_region']
#     except:
#         region = 142
#         sub_region = 145
#     ##########################################################################################################

#     print('INFO:     API Request Approved!!!')

#     ##########################################################################################################
    

#     start_date = str(start_date)
#     end_date = str(end_date)

#     ##########################################################################################################

#     cotraveler_functions = CotravelerFunctions()
#     utils = CDR_Utils(verbose=True)
#     cassandra_tools = CassandraTools()

#     ##########################################################################################################

#     start_date = utils.convert_datetime_to_ms_str(start_date)
#     end_date = utils.convert_datetime_to_ms_str(start_date)

#     ##########################################################################################################

#     # Perform analysis on devices
#     analyzer = GeoLocationAnalyzer(table_id = table_id)

#    # device_list = cotraveler_functions.separate_list(device_ids,1)
#     df_history = cassandra_tools.get_device_history_geo_chunks(device_ids, start_date, end_date,region,sub_region, server)

#     analyzer.separate_devices_into_objects(df_history)

#     # Perform AOI Detection on all devices
#     analyzer.perform_analysis_on_all_devices()

#     ##########################################################################################################
    
#     # Perform AOI Classification
#     analyzer.find_Home_AOI_for_all_devices()

#     result_aoi = analyzer.identify_work_AOI_for_all_devices()

#     #########################################################################################################

#     # Save AOI classification results
#     analyzer.save_aoi_classification_results()

#     ##########################################################################################################

#     # Perform AOI suspiciousness Analysis
#     aoi_suspiciousness_analysis = analyzer.evaluate_suspiciousness_of_all_devices()
    
#     ##########################################################################################################

#     # Perform Confidence Analysis
#     geo_confidence_analyzer = GeospatialConfidenceAnalysis()
#     print('INFO:    GEOSPATIAL CONFIDENCE ANALYSIS STARTED!')

#     confidence_analysis = geo_confidence_analyzer.evaluate_confidence_in_analysis_for_all_devices(df_history, table_id)
    
#     ##########################################################################################################

#     print('INFO:  AOI Detection & Classification Engine Successfully Completed!!!')

#     ##########################################################################################################

#     reporting = ReportGenerator(confidence_analysis)
#     reporting.get_aoi_report(analyzer, table_id)

#     ##########################################################################################################

#     print('INFO:  AOI Reports Successfully Created!!!')

# #######################################################################################################################################
# @app.post("/correlation")
# # 
# async def read_root(string_entity: str = Body(...)):

#     start_time_test = time.time()
#     data = json.loads(string_entity)
#     table_id=data['table_id']
#     imsi_id=data['id']
#     start_date=data['start_date']
#     end_date=data['end_date']
#     server=data['server']
#     try:
#         region = data['region']
#         sub_region = data['sub_region']
#     except:
#         region=142
#         sub_region=145

#     ##########################################################################################################
    
#     utils = CDR_Utils(verbose=True)
#     oracle_tools = OracleTools()
#     correlation_main = CorrelationMain()
#     report = ReportGenerator()
#     # oracle_spark_tools = OracleSparkTools()
 
    
#     ##########################################################################################################
    
#     local = True
#     distance = 500
    
#     start_date = str(start_date)
#     end_date = str(end_date)

#     start_date = utils.convert_datetime_to_ms_str(start_date)
#     end_date = utils.convert_datetime_to_ms_str(start_date)

#     ##########################################################################################################
#     try:
        
#         table, df_device = correlation_main.correlation(imsi_id=imsi_id,
#                             start_date=start_date,
#                             end_date=end_date,
#                             local=local,
#                             distance=distance,
#                             server=server,
#                             region=region,
#                             sub_region=sub_region
#                             )


#         # table.to_csv('table.csv',index=False)
#         # table = pd.read_csv('table.csv')

#         # table['COMMON_LOCATIONS_HITS'] = table['COMMON_LOCATIONS_HITS'].apply(lambda x: ','.join(map(str, x)))
#         table['COMMON_LOCATIONS_HITS'] = table['COMMON_LOCATIONS_HITS'].astype(str)
#         print(table)
#         # oracle_spark_tools.create_temp_table(df = table,table_name = properties.oracle_correlation_table_name ,table_id = table_id, clob_columns_name = 'common_locations_hits',server=server)
#         oracle_tools.drop_create_insert(table,properties.oracle_correlation_table_name ,table_id,properties._oracle_table_schema_query_correlation)
#     except Exception as e :
#         print(e)
#         table = None
#         print('Error in correlation_main.correlation')

#         return '-1'
#     ##########################################################################################################
#     report.get_cdr_report(dataframe = df_device,file_name = 'report' ,table_id = table_id)

#     end_time_test = time.time()
    
#     print("Time taken: ", (end_time_test - start_time_test) / 60, "minutes")

#     return list(table.to_dict(orient="records"))

# #######################################################################################################################################
# @app.post("/cotraveler")
# # 
# async def read_root(string_entity: str = Body(...)):
#     start_time_test = time.time()
#     data = json.loads(string_entity)
#     table_id = data['table_id']
#     device_id = data['id']
#     start_date = data['start_date']
#     end_date = data['end_date']
#     server = data['server']
#     try:
#         region = data['region']
#         sub_region = data['sub_region']
#     except:
#         region = 142
#         sub_region = 145



#     ##########################################################################################################

#     utils = CDR_Utils()
#     oracle_tools = OracleTools()
#     cotraveler_main = CotravelerMain()
#     geo_report = ReportGenerator(verbose=True)
    
#     ##########################################################################################################
#     local = True
#     distance = 300
    
#     start_date = str(start_date)
#     end_date = str(end_date)

#     start_date = utils.convert_datetime_to_ms_str(start_date)
#     end_date = utils.convert_datetime_to_ms_str(start_date)

#     ##########################################################################################################
#     start_time_test = time.time()
#     try:
#         table , df_merged_list, df_device,df_common, df_history,distance = cotraveler_main.cotraveler(device_id=device_id,
#                         start_date=start_date,
#                         end_date=end_date,
#                         local=local,
#                         distance=distance,
#                         server=server,
#                         region=region,
#                         sub_region = sub_region)
#         table['COMMON_LOCATIONS_HITS'] = table['COMMON_LOCATIONS_HITS'].astype(str)
#         table_insert = table[['RANK','DEVICE_ID','GRID_SIMILARITY','COUNT','COMMON_LOCATIONS_HITS','SEQUENCE_DISTANCE','LONGEST_SEQUENCE']]
#         table.to_csv(properties.passed_filepath_excel + 'table.csv')
#         # oracle_spark_tools.create_temp_table(table,properties.co_traveler_table ,table_id , clob_columns_name = 'COMMON_LOCATIONS_HITS',server=server)
        
#         oracle_tools.drop_create_insert(table_insert,
#                                         properties.co_traveler_table,
#                                         table_id,
#                                         properties._oracle_table_schema_query_cotraveler
#                                         )
        
#         end_time_test = time.time()
#         print("Time taken: ", (end_time_test - start_time_test) / 60, "minutes")
#         # Report
#     except Exception as e:
#         print(e)
#         table = None
#         print('Error in cotraveler_main.cotraveler')

#         return '-1'

#     ##########################################################################################################
#     geo_report.get_geo_report(dataframe = df_merged_list,df_main = df_device,df_common = df_common,table = table,df_history = df_history,report_type='cotraveler',file_name='report',table_id=table_id,distance = distance)
#     table = table[['DEVICE_ID','TOTAL_HITS']]
#     return list(table.to_dict(orient="records"))

#######################################################################################################################################
@app.post("/report")
# 
async def simulation_report(string_entity: str = Body(...)):

    start_time_test = time.time()
    data = json.loads(string_entity)
    table_id=data['table_id']
    start_date=data['start_date']
    end_date=data['end_date']
    server=data['server']


    utils = CDR_Utils(verbose=True)
    oracle_tools = OracleTools()
    report = ReportGenerator() 



    query = f"SELECT * FROM locdba.loc_report_config where LOC_REPORT_CONFIG_ID ={table_id} "

    result= oracle_tools.get_oracle_query(query)

    print(result.columns)
    report_type = result['LOC_REPORT_TYPE'][0]
    if report_type==11:
        table = oracle_tools.get_table("SSDX_ENG","DATACROWD_CDR_DATA",table_id,True)
    else:    
        table = oracle_tools.get_table("SSDX_ENG","TMP_REPORT_COORDINATES_6",table_id,True)

    print(table)
    print(table.columns)
    query = f"SELECT REPORT_TYPE , REPORT_NAME FROM TECHDBA.CYB_REPORT_HTML_TEMPLATES where REPORT_TYPE={report_type}"
    table_type = oracle_tools.get_oracle_query(query)
    report_name = table_type['REPORT_NAME'][0]
    print(report_name)

    report.get_simulation_report(main_table = result , report_table = table,table_id = table_id , file_name ='report',report_type=report_type,report_name=report_name)
 
    return "Done"   

#######################################################################################################################################
# @app.post("/chat")
# # 
# async def chat_agent(string_entity: str = Body(...)):
#     # huggingface_hub.login()
#     data = json.loads(string_entity)
#     prompt=data['prompt']
#     print(prompt)
#     utils = CDR_Utils(verbose=True)
#     agent_tools = AgentTools()
#     agent = StarCoderAgent()
#     # huggingface_hub.login()

#     tool1 = agent_tools.get_device_history()

#     tool2 = agent_tools.get_device_scan()

#     tool3 = agent_tools.get_cotraveler()
    
#     tool4 = agent_tools.get_simulation()

#     tool5 = agent_tools.get_polygon_scan_saved_shape()
#     model = agent.get_agent(tools = [tool1,tool2,tool3,tool4,tool5])
#     answer = model.chat(prompt)
#     return answer

@app.post("/timeline")
# 
async def timeline_report(string_entity: str = Body(...)):

    start_time_test = time.time()
    data = json.loads(string_entity)
    table_id=data['table_id']
    timeline_funtions = TimeLineFunctions()
    timeline_funtions.timeline(table_id=table_id)

    return "Done"   

if __name__ == "__main__":
    uvicorn.run(app, host=properties.api_host, port=properties.app_port , reload=False) 