import json
from fastapi import FastAPI, HTTPException
from typing import List, Union
from shapely.geometry import Point
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import math, os
import folium
import warnings
from fastapi.responses import HTMLResponse
from vcis.session_detection.session_detection_main import SessionDetectionMain
import uvicorn

from vcis.utils.utils import CDR_Utils,CDR_Properties
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.cotraveler.cotraveler_functions import CotravelerFunctions
from vcis.plots.cotraveler_plots import CotravelerPlots
from vcis.event_tracing.common_devices_functions import CommonDevicesFunctions

from datetime import datetime
##########################################################################################################
global properties
properties = CDR_Properties()

from pydantic import BaseModel, Field

# Create an instance of FastAPI
app = FastAPI()

# Define a Pydantic model for input validation
class InputData(BaseModel):
    imsi: str
    server: str ='10.1.10.110'
    start_date: str#datetime
    end_date: str#datetime
    fetch_size: int = 30000
    radius: int = 2000 #meters
    sectors_threshold: int = 5
    time_difference_threshold: int = 600 #minutes
    time_window: int = 180 #minutes
    new_active_location_threshold: int = 180 #minutes

    class Config:
        arbitrary_types_allowed = True
# Initialize your session detection class
session_detector = SessionDetectionMain(verbose=True)  # Adjust verbose as needed

# Define the API endpoint
@app.post("/detect-overshoots/")
def detect_overshoots(input_data: InputData):
    df = session_detector.generate_overshoot_flags(
        input_data.imsi,
        input_data.server,
        input_data.start_date,
        input_data.end_date,
        input_data.fetch_size,
        input_data.radius,
        input_data.sectors_threshold,
        input_data.time_difference_threshold,
        input_data.time_window,
        input_data.new_active_location_threshold
    )
    df_json_dict = json.loads(df.to_json(orient='records'))
    return df_json_dict


if __name__ == "__main__":
    uvicorn.run(app, host=properties.api_host, port=properties.api_port_overshooting_detection , reload=False) 
