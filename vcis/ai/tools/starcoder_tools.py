from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.ai.tools.vcis_tools import vcisTools

from transformers import Tool
import pandas as pd
import json
###################################################################################################################
global vcis_tools
vcis_tools = vcisTools()
class DeviceHistory(Tool):
    name = "get_device_history"
    description = ("This is a tool that fetches the device history from cassandra . It takes device id and start date and end date as input , and returns a dataframe from cassadnra and print it ")

    inputs = ["text"]
    outputs = ["text"]

    def __call__(self,device_id:str,start_date:str,end_date:str):

        response = vcis_tools.get_device_history_tool(device_id=device_id,start_date=start_date,end_date=end_date)

        return response
    
###################################################################################################################    
class ActivityScan(Tool):
        name = "get_activity_scan"
        description = ("This is a tool that fetches the activity scan from cassandra . It takes latitude, longitude, start date ,end date and scan distance as input , and returns number of devices retrieved")

        inputs = ["text"]
        outputs = ["text"]

        def __call__(self,latitude:float,longitude:float,start_date:str,end_date:str,scan_distance:int):

            response = vcis_tools.get_activity_scan(latitude=latitude,longitude=longitude,start_date=start_date,end_date=end_date,scan_distance=scan_distance)
            return response

###################################################################################################################      
class DeviceHistoryTravelrPattern(Tool):
        name = "get_device_history_travel_pattern"
        description = ("This is a tool that fetches the device history travel pattern of dhtp from cassandra. It takes latitude, longitude, start date ,end date and scan distance as input , and returns number of devices retrieved")

        inputs = ["text"]
        outputs = ["text"]

        def __call__(self,latitude:float,longitude:float,start_date:str,end_date:str,scan_distance:int):
           
           response =  vcis_tools.get_device_history_travel_pattern(latitude=latitude,longitude=longitude,start_date=start_date,end_date=end_date,scan_distance=scan_distance)
           return response 
        
###################################################################################################################
class Cotraveler(Tool):
    name = "get_cotraveler"
    description = ("This is a tool that fetches the cotraveler for device id from cassandra . It takes device id and start date and end date as input , and returns a dataframe for the possible cotravelers from cassadnra and print it ")

    inputs = ["text"]
    outputs = ["text"]

    def __call__(self,device_id:str,start_date:str,end_date:str):

        response = vcis_tools.get_cotraveler(device_id=device_id,start_date=start_date,end_date=end_date)

        return response
    
###################################################################################################################

class GetSimulation(Tool):
    name = "get_simulation"
    description = ("This is a tool that fetches saved simulations from oracle database by the simulation name of the simulation. It takes the simulation name of the simulation as input , and returns data saved in oracle database ")

    inputs = ["text"]
    outputs = ["text"]

    def __call__(self,simulation_name:str):

        response = vcis_tools.get_simulation(simulation_name=simulation_name)

        return response 

    
###################################################################################################################

class PolygonScanSavedShape(Tool):
    name = "get_polygon_scan_saved_shape"
    description = ("This is a tool that fetches the activity scan from cassandra for a saved shape . It takes the name of the saved shape_name ,start date and end date as input , and returns number of devices retrieved")

    inputs = ["text"]
    outputs = ["text"]

    def __call__(self,shape_name:str,start_date:str,end_date:str):
        
        response = vcis_tools.get_polygon_scan_saved_shape(shape_name=shape_name,start_date=start_date,end_date=end_date)

        return response
        
  
################################################################################################################### 

class GenerateReportFromSimulation(Tool):
    name="get_generate_report_from_simulation"
    description = ("This is a tool generate a report from saved simulation and add description to it . It takes the name of the saved simulation_name and description and returns a report")

    inputs = ["text"]
    outputs = ["text"]

    def __call__(self,simulation_name:str,description:str):
        
        response = vcis_tools.get_generate_report_from_simulation(simulation_name=simulation_name,description=description)

        return response


###################################################################################################################  

class QAOverSimulation(Tool):
    name="filter_simulation"
    description = ("This is a tool used to create plots or answer questions about the simulatio. It takes the name of the saved simulation_name and the question and returns an answer")

    inputs = ["text"]
    outputs = ["text"]

    def __call__(self,simulation_name:str,question:str):
        
        response = vcis_tools.get_qa_over_simulation(simulation_name=simulation_name,question=question)

        return response
    
###################################################################################################################
class AgentTools():
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.utils = CDR_Utils()
        self.properties = CDR_Properties()

    def get_device_history(self):
        DeviceHistory(Tool)
        tool = DeviceHistory()
        return tool
    def get_activity_scan(self):
        ActivityScan(Tool)
        tool = ActivityScan()
        return tool
    def get_device_history_travel_pattern(self):
        DeviceHistoryTravelrPattern(Tool)
        tool=DeviceHistoryTravelrPattern()
        return tool
    def get_cotraveler(self):
        Cotraveler(Tool)
        tool = Cotraveler()
        return tool
    def get_simulation(self):
        GetSimulation(Tool)
        tool = GetSimulation()
        return tool
    def get_polygon_scan_saved_shape(self):
        PolygonScanSavedShape(Tool)
        tool = PolygonScanSavedShape()
        return tool
    
    def get_generate_report_from_simulation(self):
        GenerateReportFromSimulation(self)
        tool = GenerateReportFromSimulation()
        return tool
    
    def get_qa_over_simualtion(self):
        QAOverSimulation(self)
        tool = QAOverSimulation()
        return tool
    
###################################################################################################################
