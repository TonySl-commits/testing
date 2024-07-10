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
from vcis.reverse_geocoder.reverse_geocoder_polygon import ReverseGeocoder
import uvicorn

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

from pydantic import BaseModel, Field
# Suppress the warning
warnings.filterwarnings("ignore", message="Geometry is in a geographic CRS")
# Create an instance of FastAPI
app = FastAPI()

# Get the current directory of the script
current_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(current_dir, '..', 'reverse_geocoder', 'data')
shapefile_path = os.path.join(data_dir, 'buffered_world.shp')
rg = ReverseGeocoder(shapefile_path)


class CreateCircleAroundPointRequest(BaseModel):
   center_lat: float
   center_lon: float
   radius_km: float
   num_points: int

class PointRequest(BaseModel):
    latitude: float = Field(..., example=51.5074)
    longitude: float = Field(..., example=-0.1278)

class DataFrameRequest(BaseModel):
    data: List[dict]

class ReverseGeocodeResponse(BaseModel):
    NAME: str
    country_code: str

class ListReverseGeocodeResponse(BaseModel):
    country_info: List[ReverseGeocodeResponse]

# Update endpoint functions to accept and return Pydantic model instances
@app.post("/reverse_geocode_point/", response_model=ListReverseGeocodeResponse)
async def reverse_geocode_point(point: PointRequest):
    point = Point(point.longitude, point.latitude)
    country_info = rg.reverse_geocode_point(point)
    return {"country_info": country_info}

@app.post("/reverse_geocode_multipoints/", response_model=ListReverseGeocodeResponse)
async def reverse_geocode_multipoints(data: DataFrameRequest):
    df = pd.DataFrame(data.data)
    country_info = rg.reverse_geocode_multipoints(df)
    return {"country_info": country_info}

@app.post("/reverse_geocode_with_binning/", response_model=ListReverseGeocodeResponse)
async def reverse_geocode_with_binning(data: DataFrameRequest, step_distance: int = 200):
    df = pd.DataFrame(data.data)
    country_info = rg.reverse_geocode_with_Binning(df, step_distance)
    return {"country_info": country_info}


# @app.post("/generate_incomplete_circle/", response_model=List[List[float]])
# def generate_incomplete_circle(data: CreateCircleAroundPointRequest):
#     circle_points = rg.generate_incomplete_circle(data.center_lat, data.center_lon, data.radius_km, data.num_points)
#     return circle_points

# @app.post("/plot_circle_on_map/")
# def plot_circle_on_map(data: CreateCircleAroundPointRequest):
#     m = rg.plot_circle_on_map(data.center_lat, data.center_lon, data.radius_km, data.num_points)
#     # Convert the Folium map object to HTML
#     html_map = m._repr_html_()
#     return html_map

@app.post("/check_circle_borders/", response_model=ListReverseGeocodeResponse)
async def check_circle_borders(data: CreateCircleAroundPointRequest):
    country_info = rg.check_circle_borders(data.center_lat, data.center_lon, data.radius_km, data.num_points)
    
    return {"country_info": country_info}


@app.post("/check_polygon_borders/", response_model=ListReverseGeocodeResponse)
async def check_polygon_borders(polygon: List[List[float]]):
    polygon = Polygon(polygon)
    country_info = rg.check_polygon_borders(polygon)
    return {"country_info": country_info}



if __name__ == "__main__":
    uvicorn.run(app, host=properties.api_host, port=properties.api_port_reverse_geocoder , reload=False) 
