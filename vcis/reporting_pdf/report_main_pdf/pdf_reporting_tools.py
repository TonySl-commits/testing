##############################################################################################################################
    # Imports
##############################################################################################################################

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
from plotly.subplots import make_subplots
import datapane as dp
from bs4 import BeautifulSoup
from langchain_community.llms import Ollama

from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.reporting.geo_reporting.aoi_report.aoi_report_functions import AOIReportFunctions

class ReportingTools:
    def __init__(self, verbose=False):
        self.properties = CDR_Properties()
        self.utils = CDR_Utils(verbose=True)
        self.cassandra_tools = CassandraTools()
        self.cassandra_spark_tools = CassandraSparkTools()
        self.oracle_tools = OracleTools() 
        self.verbose = verbose

##############################################################################################################################
    # Reportin Functions
##############################################################################################################################

    # Common Functions

            
    def get_exctract_content(self,text:str=None, start_marker:str=None, end_marker:str=None):
        # Find the start and end indices of the answer
        start_index = text.find(start_marker) + len(start_marker)
        if end_marker!=None:
            end_index = text.find(end_marker)
        else:
            end_index = -1

        # Extract the answer
        content = text[start_index:end_index]
        return content
    
##############################################################################################################################

