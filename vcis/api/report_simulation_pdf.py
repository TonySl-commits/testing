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
from vcis.reporting_pdf.simulation_reporting.simulation_report_generator import SimulationReportGenerator


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
    simulation_report_generator = SimulationReportGenerator()
    oracle_tools = OracleTools()
    query = f"SELECT * FROM locdba.loc_report_config a where a.loc_report_config_id ={table_id}  "
    table= oracle_tools.get_oracle_query(query)
    report_type = table['LOC_REPORT_TYPE'][0]
    table_id = table['LOC_REPORT_CONFIG_ID'][0]
    report_name = table['LOC_REPORT_NAME'][0]
    start_date = table['FILTER_BDATE'][0]
    end_date = table['FILTER_EDATE'][0]

    if report_type==1:
        simulation_report_generator.get_simulation_report_ac(table_id= table_id, table = table)
    else:
        simulation_report_generator.get_simulation_report_dh(table_id=	table_id, table= table,start_date = start_date, end_date = end_date)
    return "Done"

if __name__ == "__main__":
    uvicorn.run(app, host=properties.api_host, port=properties.api_report_pdf , reload=False) # properties.api_report_pdf

