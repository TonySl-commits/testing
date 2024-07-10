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
@app.post("/gps_correlation")
async def gps_correlation(string_entity: str = Body(...)):

    start_time_test = time.time()
    data = json.loads(string_entity)
    device_id_list = data['device_id_list']
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
    
    utils = CDR_Utils(verbose=True)
    oracle_tools = OracleTools()
    correlation_main = CorrelationMain()
    report = ReportGenerator()
    
    ##########################################################################################################
    
    local = True
    distance = 30
    
    start_date = str(start_date)
    end_date = str(end_date)

    start_date = utils.convert_datetime_to_ms_str(start_date)
    end_date = utils.convert_datetime_to_ms_str(end_date)

    ##########################################################################################################
    try:
        top_id = correlation_main.gps_correlation(
                    device_id_list=device_id_list,
                    start_date=start_date,
                    end_date=end_date,
                    local=local,
                    distance=distance,
                    server=server,
                    region=region,
                    sub_region=sub_region
                    )

        if top_id is None:
            raise Exception("No data found for the provided device ID list.")

        print("Top ID:", top_id)
    except Exception as e:
        print(e)
        return '-1'
    ##########################################################################################################
    end_time_test = time.time()
    
    print("Time taken: ", (end_time_test - start_time_test) / 60, "minutes")

    return json.dumps(top_id)

#if main function is called from and take port from utils
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3972)
