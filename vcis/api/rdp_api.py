# importing
from datetime import datetime
import pandas as pd
from cassandra.cluster import Cluster
from IPython.display import display
from fastapi import FastAPI, Body
import uvicorn
import time 
import numpy as np
import rdp
from cassandra.concurrent import execute_concurrent
from cassandra.cluster import Cluster
import warnings
import json
from vcis.utils.utils import CDR_Properties, CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools

global properties
properties= CDR_Properties()
cassandra = CassandraTools()
from vcis.utils.utils import CDR_Properties, CDR_Utils

utils = CDR_Utils()
warnings.filterwarnings('ignore')   
app = FastAPI()
@app.post("/rdp")

async def read_root(string_entity: str  = Body(...)):
    start_time_test = time.time()
    data = json.loads(string_entity)
    device_id_list=data['id_list']
    start_date=data['start_date']
    end_date=data['end_date']
    server=data['server']

    start_date = str(start_date)
    end_date = str(end_date)

    start_date = utils.convert_datetime_to_ms_str(start_date)
    end_date = utils.convert_datetime_to_ms_str(end_date)

    ############################
    # Modify ###################
    tolerance = 0.00078 ########
    min_angle = np.pi *0.1 #####
    ############################
    session  =cassandra.get_cassandra_connection(server)
    df_devices = cassandra.get_device_history_geo_chunks(device_id_list,start_date=start_date,end_date=end_date, session=session)
    print(df_devices)
    # df_device = pd.read_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/data_zaher.csv")

    # print(df_device.columns)

    def add_date(data,timeframe_column):
        data = data.sort_values(timeframe_column)
        data['date'] = pd.to_datetime(data[timeframe_column], unit='ms')
        data['day'] = data['date'].dt.date
        return data

    def angle(dir):
        dir2 = dir[1:]
        dir1 = dir[:-1]
        return np.arccos((dir1*dir2).sum(axis=1)/(
            np.sqrt((dir1**2).sum(axis=1)*(dir2**2).sum(axis=1))))

    def apply_rdp(group):
        points = group[['location_latitude', 'location_longitude']].values.tolist()
        simplified_points = rdp.rdp(points, epsilon=tolerance)
        simplified_df = pd.DataFrame(simplified_points, columns=['location_latitude', 'location_longitude'])
        simplified_df['day'] = group['day'].iloc[0]
        
        location_timeframes = group.set_index(['location_latitude', 'location_longitude'])['usage_timeframe'].to_dict()
        simplified_df['usage_timeframe'] = simplified_df.apply(lambda row: location_timeframes.get((row['location_latitude'], row['location_longitude']), None), axis=1)
        
        simplified_df['simplified_points'] = simplified_points
        return simplified_df
    
    simplified_devices_df = pd.DataFrame()
    for device_id in device_id_list:
        df_device = df_devices[df_devices['device_id'] == device_id]
        df_device = add_date(df_device,'usage_timeframe')
        pp_df = df_device.groupby('day')
        simplified_df = pp_df.apply(apply_rdp).reset_index(drop=True)


        simplified_points = np.array(simplified_df['simplified_points'].values.tolist())
        directions = np.diff(simplified_points, axis=0)
        theta = angle(directions)
        idx = np.where(theta>min_angle)[0]+1
        indices =idx.tolist()

        df_filtered = simplified_df.iloc[indices]

        merged_df = pd.merge(df_device,df_filtered, on=['location_longitude', 'location_latitude', 'usage_timeframe','day'], how='left', indicator=True)
        merged_df['label'] = np.where(merged_df._merge == 'both', 'corner', 'point')
        df_device=merged_df.drop(['_merge','simplified_points'],axis=1)

        df_device = add_date(df_device,'usage_timeframe')
        df_device = df_device[['device_id', 'location_latitude', 'location_longitude', 'usage_timeframe','label']]
        print(df_device[df_device['label'] == 'corner'])
        end_time_test = time.time()
        print(df_device['label'].unique())
        print("Time taken: ", end_time_test - start_time_test, "seconds")

        simplified_devices_df = pd.concat([simplified_devices_df,df_device])
    print(simplified_devices_df)
    return list(simplified_devices_df.to_dict(orient="records"))

if __name__ == "__main__":
    uvicorn.run(app, host= properties.api_host, port=properties.api_port_rdp, reload=False)

    