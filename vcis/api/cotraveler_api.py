import uvicorn
from fastapi import FastAPI, Body
import time
import json
import warnings
warnings.filterwarnings('ignore')

from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.cotraveler.cotraveler_main import CotravelerMain
from vcis.reporting.report_main.report_generator import ReportGenerator
from vcis.reporting.report_main.load_or_create_simulation_reprot import LoadOrCreateSimulationReport

##########################################################################################################
global properties
properties= CDR_Properties()

app = FastAPI()
@app.post("/cotraveler")
# 
async def read_root(string_entity: str = Body(...)):
    start_time_test = time.time()
    data = json.loads(string_entity)
    table_id = data['table_id']
    device_id = data['id']
    start_date = data['start_date']
    end_date = data['end_date']
    server = data['server']
    try:
        region = data['region']
        sub_region = data['sub_region']
    except:
        region = 142
        sub_region = 145



    ##########################################################################################################

    utils = CDR_Utils()
    oracle_tools = OracleTools()
    cotraveler_main = CotravelerMain()
    geo_report = ReportGenerator(verbose=True)
    load_or_create_simulation_report = LoadOrCreateSimulationReport()

    ##########################################################################################################
    local = True
    distance = 300
    
    start_date = str(start_date)
    end_date = str(end_date)

    start_date = utils.convert_datetime_to_ms_str(start_date)
    end_date = utils.convert_datetime_to_ms_str(end_date)

    ##########################################################################################################
    start_time_test = time.time()
    # try:
    table , df_merged_list ,df_device ,df_common,df_history, cotraveler_distance,heatmap_plots,cotraveler_barchart,cotraveler_user_prompt = cotraveler_main.cotraveler(device_id=device_id,
                    start_date=start_date,
                    end_date=end_date,
                    local=local,
                    distance=distance,
                    server=server,
                    region=region,
                    sub_region = sub_region)
    table['COMMON_LOCATIONS_HITS'] = table['COMMON_LOCATIONS_HITS'].astype(str)
    print(table,table.columns)
    table_insert = table[['RANK','DEVICE_ID','GRID_SIMILARITY','COUNT','COMMON_LOCATIONS_HITS','SEQUENCE_DISTANCE','LONGEST_SEQUENCE']]
    table.to_csv(properties.passed_filepath_excel + 'table.csv')
    # oracle_spark_tools.create_temp_table(table,properties.co_traveler_table ,table_id , clob_columns_name = 'COMMON_LOCATIONS_HITS',server=server)
    
    oracle_tools.drop_create_insert(table_insert,
                                    properties.co_traveler_table,
                                    table_id,
                                    properties._oracle_table_schema_query_cotraveler
                                    )
    
    end_time_test = time.time()
    print("Time taken: ", (end_time_test - start_time_test) / 60, "minutes")
        # Report
    # except Exception as e:
    #     print(e)
    #     table = None
    #     print('Error in cotraveler_main.cotraveler')

    #     return '-1'

    ##########################################################################################################
    geo_report.get_geo_report(dataframe = df_merged_list,df_main = df_device,df_common = df_common,table = table,df_history = df_history,report_type='cotraveler',file_name='report',table_id=table_id,distance = distance)
    load_or_create_simulation_report.load_or_create_simulation_report(table_id=table_id)
    table = table[['DEVICE_ID','TOTAL_HITS']]
    return list(table.to_dict(orient="records"))

##########################################################################################################

#   python -m uvicorn cotraveler:app --host 10.10.10.60 --port 8080 --reload
if __name__ == "__main__":

    uvicorn.run(app, host= properties.api_host, port=properties.api_port_cotraveler, reload=False)