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
from vcis.reporting.report_main.report_generator import ReportGenerator

##########################################################################################################
global properties
properties = CDR_Properties()
app = FastAPI()
@app.post("/report")
# 
async def read_root(string_entity: str = Body(...)):

    start_time_test = time.time()
    data = json.loads(string_entity)
    table_id=data['table_id']
    # start_date=data['start_date']
    # end_date=data['end_date']
    # server=data['server']


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
    report.get_simulation_report(main_table = result , report_table = table,table_id = table_id , file_name ='simulation_report',report_type=report_type,report_name=report_name)
    
    return "Done"   
if __name__ == "__main__":
    uvicorn.run(app, host=properties.api_host, port=properties.api_report , reload=False) 
