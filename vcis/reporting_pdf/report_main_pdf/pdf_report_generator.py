##############################################################################################################################
    # Imports
##############################################################################################################################

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
from plotly.subplots import make_subplots
import datapane as dp

from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.reporting.report_main.reporting_tools import ReportingTools
from vcis.reporting.geo_reporting.cotraveler_report.cotraveler_report_generator import CotravelerReportGenerator
from vcis.reporting.cdr_reporting.cdr.cdr_report_generator import CdrReportGenerator
from vcis.reporting.simulation_reporting.simulation_report_generator import SimulationReportGenerator
from vcis.reporting.simulation_reporting.simulation_report_functions import SimulationReportFunctions
from vcis.reporting.geo_reporting.aoi_report.aoi_report_functions import AOIReportFunctions

##############################################################################################################################
    # GEO Reporting Functions
##############################################################################################################################

class ReportGenerator():
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.properties = CDR_Properties()
        self.utils = CDR_Utils(verbose=self.verbose)
        self.cassandra_tools = CassandraTools()
        self.cassandra_spark_tools = CassandraSparkTools()
        self.oracle_tools = OracleTools()
        self.reporting_tools = ReportingTools(self.verbose)
        self.cotraveler_reprot_generator = CotravelerReportGenerator(self.verbose)
        self.cdr_report_generator = CdrReportGenerator()
        self.simulation_report_generator = SimulationReportGenerator()
        self.simulation_report_functions = SimulationReportFunctions()
        self.blocks = []
        self.blocks_aoi = []
        
    def get_geo_report(self, dataframe:pd.DataFrame=None ,df_main:pd.DataFrame=None, table:pd.DataFrame=None, df_history:pd.DataFrame=None, report_type:str=None, file_name:str=None, table_id:str=None,distance:int = 50):
        device_id_main = df_main['device_id'].iloc[0]
        
        kpis_blocks = self.cotraveler_reprot_generator.get_cotraveler_kpis(table)

        common_countries_map = self.cotraveler_reprot_generator.get_common_countires_timeline(table)
        self.blocks.append(common_countries_map) 
        figure_time_spent_main_list, figure_time_spent_cotraveler_list= self.cotraveler_reprot_generator.get_time_spent_percentage(df = dataframe,df_main = df_main,table = table,df_history = df_history)
        
        self.blocks.append(figure_time_spent_main_list)
        
        self.blocks.append(figure_time_spent_cotraveler_list)
        
        figure_Mapping_Shared_Instances_Across_Time_list  = self.cotraveler_reprot_generator.get_Mapping_Shared_Instances_Across_Time(dataframe,table,distance)
        
        self.blocks.append(figure_Mapping_Shared_Instances_Across_Time_list)
        
        self.blocks = [[sublist[i] for sublist in self.blocks] for i in range(len(self.blocks[0]))]

        report = self.reporting_tools.get_report_layout(self.blocks,kpis_blocks,table,device_id_main,report_type)
        
        self.reporting_tools.save_report(report = report ,report_path = self.properties.passed_filepath_reports_html,report_name = self.properties.passed_report_name_html_geo,open=True)
        
        script_contents = self.reporting_tools.get_head_scripts(self.properties.passed_filepath_reports_html+self.properties.passed_report_name_html_geo+'.html')
        self.oracle_tools.drop_create_table(table_name = self.properties.oracle_analytcis_report_table ,table_id = table_id , table_schema_query = self.properties._oracle_table_schema_query_analytics_report) 
        self.reporting_tools.insert_into_database(file_name , script_contents,table_id)
        
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Rporting : Mapping Shared Instances Across Time Report Complete'))

    def get_cdr_report(self, dataframe:pd.DataFrame=None ,report_type:str=None, file_name:str=None, table_id:str=None):
        cgi_count_historgram = self.cdr_report_generator.get_cgi_count_histogram(dataframe)
        
        self.blocks.append(cgi_count_historgram)
        
        cgi_usage_folium ,bts_table = self.cdr_report_generator.get_map_cgi_usage_folium(dataframe)
        
        dataframe = pd.merge(dataframe, bts_table, on='cgi_id', how='left')
        self.blocks.append(cgi_usage_folium)
        self.blocks.append(dataframe[['cgi_id','usage_timeframe','network_type_code','bts_cell_name','site_name','uafrcn_code','location_azimuth','location_latitude','location_longitude']])
        
        report = self.reporting_tools.get_report_layout_cdr(self.blocks,dataframe,report_type)

        self.reporting_tools.save_report(report = report ,report_path = self.properties.passed_filepath_reports_html,report_name = self.properties.passed_report_name_html_cdr,open=True)
        
        script_contents = self.reporting_tools.get_head_scripts(self.properties.passed_filepath_reports_html+self.properties.passed_report_name_html_cdr+'.html')
        
        self.oracle_tools.drop_create_table(table_name = self.properties.oracle_analytcis_report_table ,table_id = table_id , table_schema_query = self.properties._oracle_table_schema_query_analytics_report)
         
        self.reporting_tools.insert_into_database(file_name , script_contents,table_id)
        
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★'.format('Rporting : CDR Report Complete'))

    def get_aoi_report(self, geolocation_analyzer, table_id):

        self.geolocation_analyzer = geolocation_analyzer

        for device in range(self.geolocation_analyzer.geolocation_data_list.get_length()):
            geolocation_data = self.geolocation_analyzer.geolocation_data_list[device]

            aoi_report = AOIReportFunctions()
            aoi_report.initialize_aoi_report(geolocation_data)

            blocks = self.reporting_tools.get_report_layout_aoi(aoi_report)

            blocks_list = {aoi_report.geolocation_data.device_id_geo : blocks}

            self.blocks_aoi.append(blocks_list)
            
        report = self.reporting_tools.merge_reports_to_select_dropdown(self.blocks_aoi)

        self.reporting_tools.save_report(report = report, report_path = self.properties.passed_filepath_reports_html, report_name = self.properties.passed_report_name_html_aoi,open=True)

        script_contents = self.reporting_tools.get_head_scripts(self.properties.passed_filepath_reports_html + self.properties.passed_report_name_html_aoi + ".html")

        self.oracle_tools.drop_create_table(table_name = self.properties.oracle_analytcis_report_table, table_id = table_id , table_schema_query = self.properties._oracle_table_schema_query_analytics_report)
         
        self.reporting_tools.insert_into_database('aoi_report' , script_contents,table_id)
    def get_simulation_report(self, main_table:pd.DataFrame=None, report_table:pd.DataFrame=None, report_type:int=None, file_name:str=None ,table_id:int = None,report_name:str=None , description:str=None):
        
        number_of_hits_server_providers = self.simulation_report_generator.get_number_of_hits_per_server_provider(report_table)
        self.blocks.append(number_of_hits_server_providers)
        
        report_table = self.simulation_report_functions.process_data(report_table,report_type)
        number_of_hits_per_dow = self.simulation_report_generator.get_number_of_hits_per_dow(report_table)
        self.blocks.append(number_of_hits_per_dow)

        number_of_hits_per_month = self.simulation_report_generator.get_number_of_hits_per_month(report_table)
        self.blocks.append(number_of_hits_per_month)

        number_of_hits_per_hod = self.simulation_report_generator.get_number_of_hits_per_hod(report_table)
        self.blocks.append(number_of_hits_per_hod)

        summary_statistics = self.simulation_report_generator.get_summary_statistics(report_table,report_type)
        self.blocks.append(summary_statistics)
        if report_type==11:
            number_of_hits_per_cgi = self.simulation_report_generator.get_number_of_hits_per_cgi(report_table)
            self.blocks.append(number_of_hits_per_cgi)
        report,report_intoduction = self.reporting_tools.get_simulation_report_layout(main_table = main_table,report_table = report_table,report_name=report_name,report_type=report_type ,blocks=self.blocks,description=description)

        self.reporting_tools.save_report(report = report ,report_path = self.properties.passed_filepath_reports_html,open=True,report_name=self.properties.passed_report_name_html_simulation)
        
        script_contents = self.reporting_tools.get_head_scripts(self.properties.passed_filepath_reports_html+self.properties.passed_report_name_html_simulation+'.html')
        
        self.oracle_tools.drop_create_table(table_name = self.properties.oracle_analytcis_report_table ,table_id = table_id , table_schema_query = self.properties._oracle_table_schema_query_analytics_report)
        # self.oracle_tools.drop_create_table(table_name = self.properties.oracle_analytics_introduction_table,table_id = table_id , table_schema_query = self.properties._oracle_table_schema_query_case_report)

        self.reporting_tools.insert_into_database(file_name , script_contents,table_id)
        # self.reporting_tools.insert_intro_to_database(report_intoduction,report_name,table_id)
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Rporting : Simulation Report Complete'))
        print(report_intoduction,'report_intoduction')

        return report_intoduction,report
    

    #########################################################################
    
    def get_case_report(self, main_table:pd.DataFrame=None, report_table:pd.DataFrame=None, report_type:int=None, file_name:str=None ,table_id:int = None,report_name:str=None , description:str=None):
        
        # number_of_hits_server_providers = self.simulation_report_generator.get_number_of_hits_per_server_provider(report_table)
        # self.blocks.append(number_of_hits_server_providers)
        
        # report_table = self.simulation_report_functions.process_data(report_table,report_type)
        # number_of_hits_per_dow = self.simulation_report_generator.get_number_of_hits_per_dow(report_table)
        # self.blocks.append(number_of_hits_per_dow)

        # number_of_hits_per_month = self.simulation_report_generator.get_number_of_hits_per_month(report_table)
        # self.blocks.append(number_of_hits_per_month)

        # number_of_hits_per_hod = self.simulation_report_generator.get_number_of_hits_per_hod(report_table)
        # self.blocks.append(number_of_hits_per_hod)

        # summary_statistics = self.simulation_report_generator.get_summary_statistics(report_table,report_type)
        # self.blocks.append(summary_statistics)
        # if report_type==11:
        #     number_of_hits_per_cgi = self.simulation_report_generator.get_number_of_hits_per_cgi(report_table)
        #     self.blocks.append(number_of_hits_per_cgi)
        report,report_intoduction = self.reporting_tools.get_simulation_report_layout(main_table = main_table,report_table = report_table,report_name=report_name,report_type=report_type ,blocks=self.blocks,description=description)

        self.reporting_tools.save_report(report = report ,report_path = self.properties.passed_filepath_reports_html,open=True,report_name=self.properties.passed_report_name_html_simulation)
        
        script_contents = self.reporting_tools.get_head_scripts(self.properties.passed_filepath_reports_html+self.properties.passed_report_name_html_simulation+'.html')
        
        self.oracle_tools.drop_create_table(table_name = self.properties.oracle_analytcis_report_table ,table_id = table_id , table_schema_query = self.properties._oracle_table_schema_query_analytics_report)
        # self.oracle_tools.drop_create_table(table_name = self.properties.oracle_analytics_introduction_table,table_id = table_id , table_schema_query = self.properties._oracle_table_schema_query_case_report)

        self.reporting_tools.insert_into_database(file_name , script_contents,table_id)
        # self.reporting_tools.insert_intro_to_database(report_intoduction,report_name,table_id)
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Rporting : Simulation Report Complete'))
        print(report_intoduction,'report_intoduction')

        return report_intoduction,report
        
