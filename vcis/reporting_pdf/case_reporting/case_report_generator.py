##############################################################################################################################
    # Imports
##############################################################################################################################

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
from plotly.subplots import make_subplots
import datapane as dp
from datetime import timedelta
import folium
from folium.plugins import Geocoder , MeasureControl ,MousePosition,TimestampedGeoJson
import math
import ast
import plotly.express as px
import plotly.figure_factory as ff

from vcis.utils_AI.properties_ai import AI_Properties
from vcis.utils_AI.utils_ai import AI_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.reporting.report_main.reporting_tools import ReportingTools
from vcis.reporting.simulation_reporting.simulation_report_functions import SimulationReportFunctions

class CaseReportGenerator():
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.properties = AI_Properties()
        self.utils = AI_Utils(verbose=self.verbose)
        self.cassandra_tools = CassandraTools()
        self.cassandra_spark_tools = CassandraSparkTools()
        self.reporting_tools = ReportingTools(self.verbose)
        self.simulation_report_functions = SimulationReportFunctions()
        self.abtsract=self.properties.abstract_case_template## should we doit in AI?

    def Generate_case_intro(self, case_name, simulation_name,comments):
        user_input = self.utils.generate_intro_user_input(case_name=case_name, simulation_name=simulation_name, comments=comments)
        system_prompt=self.properties.case_intro_system_prompt
        full_prompt = f"{system_prompt}{user_input}"
        response = self.properties.llm.invoke(full_prompt)
        content = response.content.strip()
        return content
    
    def Generate_case_description(self,table_id:list):
            for id in table_id:
                     introductions=[]
                     query = f"select * from ssdx_tmp.tmp_Analytics_Report_{table_id} "
                     result= self.oracle_tools.get_oracle_query(query)
                     introduction=result['INTRODUCTION'][0]
                     introductions.append(introduction)
            user_input = self.utils.generate_case_user_input(introductions)
            system_prompt=self.properties.case_report_system_prompt
            full_prompt = f"{system_prompt}{user_input}"
            response = self.properties.llm.invoke(full_prompt)
            content = response.content.strip()
            return content
        
    



    # def generate_abstract(self,case_name: str, simulation_name: list, comments: str):
    #     generate
        
##############################################################################################################################
    # Reporting Functions
##############################################################################################################################
    def get_number_of_hits_per_server_provider(self, report_table):
        fig = self.simulation_report_functions.hits_per_service_provider_id(report_table)
        return fig
    
    def get_number_of_hits_per_dow(self,report_table):
        fig = self.simulation_report_functions.number_of_hits_per_dow(report_table)
        return fig

    def get_number_of_hits_per_month(self,report_table):
        fig = self.simulation_report_functions.number_of_hits_per_month(report_table)
        return fig 
    
    def get_number_of_hits_per_hod(self,report_table):
        fig = self.simulation_report_functions.number_of_hits_per_hod(report_table)
        return fig 
    
    def get_summary_statistics(self, report_table,report_type):
            
            if report_type==11:
                summary_stats = report_table.groupby('IMSI_ID').agg(Number_of_Hits=('Timestamp', 'count'),
                                                        Number_of_Days=('Date', 'nunique')).reset_index()
            
                summary_stats.columns = ['Device ID', 'Number of Hits', 'Number of Days']

            else:
                report_table_stat = report_table.copy()
                report_table_stat.columns = report_table_stat.columns.str.lower()

                report_table_stat = self.utils.add_reverseGeocode_columns(report_table_stat)
                print(report_table_stat.columns)
                # Group by device id and calculate the number of hits and days recorded for each device
                # Group by 'device_id' and calculate summary statistics
                summary_stats = report_table_stat.groupby('device_id').agg(
                    Number_of_Hits=('timestamp', 'count'),
                    Number_of_Days=('date', 'nunique'),
                    Number_of_countries=('country', 'nunique'),
                    Number_of_cities=('city', 'nunique'),
                    List_of_countries=('country', lambda x: list(x.unique())),  # Concatenate unique countries
                    List_of_cities=('city', lambda x: list(x.unique()))          # Concatenate unique cities
                ).reset_index()

                print(summary_stats)

                summary_stats.columns = ['Device ID', 'Number of Hits', 'Number of Days','Number Of Countries','Number Of Cities','Country list','City list']
            print('INFO:    SUMMARY STATISTICS\n', summary_stats)

            return summary_stats
    
    def get_number_of_hits_per_cgi(self,report_table):
        hits_per_cgi_id_type = report_table.groupby(['CGI_ID', 'TYPE_ID']).size().reset_index(name='NumberOfHits')

        # Create the Icicle Chart
        fig = px.icicle(hits_per_cgi_id_type, path=['TYPE_ID', 'CGI_ID'], values='NumberOfHits')
        fig.update_traces(root_color="lightgrey")
        fig.update_layout(margin=dict(t=50, l=25, r=25, b=25))
        return fig
