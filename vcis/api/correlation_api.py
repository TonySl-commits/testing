import uvicorn
from fastapi import FastAPI, Body
import time
import json
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.correlation.correlation_main import CorrelationMain
from vcis.reporting.report_main.report_generator import ReportGenerator

##########################################################################################################
global properties
properties = CDR_Properties()

app = FastAPI()
@app.post("/correlation")
# 
async def read_root(string_entity: str = Body(...)):

    start_time_test = time.time()
    data = json.loads(string_entity)
    table_id=data['table_id']
    imsi_id=data['id']
    start_date=data['start_date']
    end_date=data['end_date']
    server=data['server']
    try:
        region = data['region']
        sub_region = data['sub_region']
    except:
        region=142
        sub_region=145

    ##########################################################################################################
    
    utils = CDR_Utils(verbose=True)
    oracle_tools = OracleTools()
    correlation_main = CorrelationMain()
    report = ReportGenerator()
    # oracle_spark_tools = OracleSparkTools()
 
    
    ##########################################################################################################
    
    local = True
    distance = 500
    
    start_date = str(start_date)
    end_date = str(end_date)

    start_date = utils.convert_datetime_to_ms_str(start_date)
    end_date = utils.convert_datetime_to_ms_str(end_date)

    ##########################################################################################################
    try:
        
        table, df_device = correlation_main.cdr_correlation(imsi_id=imsi_id,
                            start_date=start_date,
                            end_date=end_date,
                            local=local,
                            distance=distance,
                            server=server,
                            region=region,
                            sub_region=sub_region
                            )


        # table.to_csv('table.csv',index=False)
        # table = pd.read_csv('table.csv')

        # table['COMMON_LOCATIONS_HITS'] = table['COMMON_LOCATIONS_HITS'].apply(lambda x: ','.join(map(str, x)))
        table['COMMON_LOCATIONS_HITS'] = table['COMMON_LOCATIONS_HITS'].astype(str)
        print(table)
        # oracle_spark_tools.create_temp_table(df = table,table_name = properties.oracle_correlation_table_name ,table_id = table_id, clob_columns_name = 'common_locations_hits',server=server)
        oracle_tools.drop_create_insert(table,properties.oracle_correlation_table_name ,table_id,properties._oracle_table_schema_query_correlation)
    except Exception as e :
        print(e)
        table = None
        print('Error in correlation_main.correlation')

        return '-1'
    ##########################################################################################################
    report.get_cdr_report(dataframe = df_device,file_name = 'report' ,table_id = table_id)

    end_time_test = time.time()
    
    print("Time taken: ", (end_time_test - start_time_test) / 60, "minutes")

    return list(table.to_dict(orient="records"))

##########################################################################################################

#   python -m uvicorn correlation:app --host 10.10.10.60 --port 8000 --reload

# if __name__ == "__main__":
#     uvicorn.run(app, host=properties.api_host, port=properties.api_port_correlation , reload=False) 