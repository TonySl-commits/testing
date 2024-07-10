import json
import uvicorn

import warnings
warnings.filterwarnings('ignore')

from fastapi import FastAPI, Body

from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.geo_trace.geo_trace import GeoTrace

# python -m uvicorn aoi-oop:app --host 10.1.8.87 --port 8000 --reload

##########################################################################################################

app = FastAPI()

@app.post("/trace")

async def read_root(string_entity: str = Body(...)):

    # Get the api data
    api_data = json.loads(string_entity)

    device_ids = api_data['device_id']
    server = api_data['server']
    simulation_id = api_data['simulation_id']
    start_date = api_data['start_date']
    end_date = api_data['end_date']
    
    ##########################################################################################################

    print('INFO:     API Request Approved!!!')

    ##########################################################################################################
    
    start_date = str(start_date)
    end_date = str(end_date)

    ##########################################################################################################

    utils = CDR_Utils()
    properties = CDR_Properties()
    cassandra_tools = CassandraTools()
    geo_tracer = GeoTrace()

    ##########################################################################################################

    start_date = utils.convert_datetime_to_ms_str(start_date)
    end_date = utils.convert_datetime_to_ms_str(start_date)

    ##########################################################################################################

    df_history = cassandra_tools.get_device_history_geo_chunk(device_ids, start_date, end_date, server = server)
   
    ##########################################################################################################

    print('INFO:       Geo Trace Engine Process Launched!!!')

    traced_paths = geo_tracer.trace(df_history)

    print('INFO:       Geo Trace Engine Process Successfully Completed!!!')
    
    traced_paths.to_json(properties.passed_filepath_json_paths + f'trace_simulation_{simulation_id}.json', orient='records', force_ascii=False)
    
    traced_paths_json = traced_paths.to_json(orient='records', force_ascii=False)
    
    # print(traced_paths_json)
    
    return traced_paths_json

##########################################################################################################
    
if __name__ == "__main__":
    uvicorn.run(app, host = "10.1.10.110", port= 3007, reload=False)