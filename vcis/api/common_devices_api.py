import uvicorn
from fastapi import FastAPI, Body
import time
import json
import pandas as pd
import warnings
import plotly.graph_objs as go
import plotly.io as pio
import base64

warnings.filterwarnings('ignore')

from vcis.utils.utils import CDR_Utils,CDR_Properties
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.cotraveler.cotraveler_functions import CotravelerFunctions
from vcis.plots.cotraveler_plots import CotravelerPlots
from vcis.event_tracing.common_devices_functions import CommonDevicesFunctions

import datetime
##########################################################################################################
global properties
properties = CDR_Properties()

app = FastAPI()
@app.post("/cdv")
# 
async def read_root(string_entity: str = Body(...)):

    start_time_test = time.time()
    data = json.loads(string_entity)
    case_id=data['case_id']
    start_date=data['start_date']
    end_date= datetime.datetime.now().strftime("%Y-%m-%d")
    print(end_date)
    server=data['server']
    try:
        region = data['region']
        sub_region = data['sub_region']
    except:
        region=142
        sub_region=145

    ##########################################################################################################
    
    utils = CDR_Utils(verbose=True)
    oracle_tools = OracleTools(verbose=True)
    cassandra_tools = CassandraTools()
    common_devices_functions = CommonDevicesFunctions()
    ##########################################################################################################
    
    local = True
    distance = 100
    
    start_date = str(start_date)
    end_date = str(end_date)

    start_date = utils.convert_datetime_to_ms_str(start_date)
    end_date = utils.convert_datetime_to_ms_str(end_date)
    session = cassandra_tools.get_cassandra_connection(server)
    step_lat,step_lon = utils.get_step_size(distance)
    # ##########################################################################################################
    # try:
    table = oracle_tools.get_table("SSDX_ENG","TMP_REPORT_COORDINATES_6",case_id,True)
    d_list = table['DEVICE_ID'].unique().tolist()

    df_history = cassandra_tools.get_device_history_geo_chunks(d_list,start_date=start_date,end_date=end_date,session=session,region=region,sub_region=sub_region)
    df_history = utils.binning(df_history,100)
    df_history = utils.combine_coordinates(df_history)


    # df_history.to_csv('df_history.csv')
    # # read df_history
    # df_history = pd.read_csv('df_history.csv')
    df_common = common_devices_functions.merge_per_device(df_history,table,step_lat = step_lat,step_lon = step_lon)


    df_common = utils.binning(df_common,100)
    df_common = utils.combine_coordinates(df_common)
    df_common = df_common.drop_duplicates()
    # df_common.to_csv('df_common.csv')
    # df_common = pd.read_csv('df_common.csv')
    groups = common_devices_functions.get_groups(df_common)
    groups['group_ids'] = groups['group_ids'].astype(str)

    import random

    # Define the range for random milliseconds (1 day to 30 days in milliseconds)
    one_day_ms = 24 * 60 * 60 * 1000
    thirty_days_ms = 30 * one_day_ms

    # Adjust USAGE_TIMEFRAME by adding or subtracting a random number of milliseconds within the specified range
    groups['USAGE_TIMEFRAME'] = groups['USAGE_TIMEFRAME'].apply(
        lambda x: str(int(x) + random.randint(-thirty_days_ms, thirty_days_ms))
    )

    groups['USAGE_TIMEFRAME'] = groups['USAGE_TIMEFRAME'].astype(str)
    groups['Group_ID'] = range(len(groups))

    groups = common_devices_functions.get_graphs_for_groups(df_history, groups)

    stat_table = common_devices_functions.get_stat_table(df_common)
    stat_table_html = utils.add_css_to_html_table(stat_table)
    # groups['HTML_GRAPHS'] = groups['HTML_GRAPHS'].apply(lambda x: base64.b64encode(x.encode('utf-8')).decode('utf-8'))

    df_common = df_common.drop(columns=['longitude_grid','latitude_grid','grid'],axis=1)
    oracle_tools.drop_create_insert(df_common,properties.common_devices_table_name ,case_id,properties._oracle_table_schema_query_common_devices)
    oracle_tools.drop_create_insert(groups,properties.common_devices_group_table_name ,case_id,properties._oracle_table_schema_query_common_devices_group)

    # except Exception as e :
    #     print(e)
    #     table = None
    #     print('Error in common_devices_api')

    #     return '-1'
    ##########################################################################################################

    end_time_test = time.time()
    
    print("Time taken: ", (end_time_test - start_time_test) / 60, "minutes")

    return json.dumps({"html_content" : stat_table_html,"groups": groups.to_dict(orient="records")})
##########################################################################################################

#   python -m uvicorn correlation:app --host 10.10.10.60 --port 8000 --reload

if __name__ == "__main__":
    uvicorn.run(app, host=properties.api_host, port=properties.api_port_common_devices , reload=False) 


    # figures = {}
    # for device_id in df_common['main_id'].unique():
    #     df_main = df_history[df_history['device_id'] == device_id]
    #     df_common_filtered = df_common[df_common['main_id'] == device_id]

    #     df_main = df_main.sort_values('usage_timeframe')
    #     df_common_filtered = df_common_filtered.sort_values('usage_timeframe')

    #     df_main = df_main.reset_index(drop=True)
    #     df_common_filtered = df_common_filtered.reset_index(drop = True)

    #     figure = common_devices_functions.get_barchart_plots(df_main,df_common_filtered)

    #     figure_html = pio.to_html(figure, full_html=False)
    #     figures[device_id] = figure_html