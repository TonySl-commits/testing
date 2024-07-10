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
from vcis.timeline.timeline_functions import TimeLineFunctions
from vcis.reporting.simulation_reporting.simulation_report_generator import SimulationReportGenerator
from vcis.reporting.simulation_reporting.simulation_report_functions import SimulationReportFunctions

from vcis.aoi_classification.geolocation_data_analyzer import GeoLocationAnalyzer
from vcis.reporting.geo_reporting.aoi_report.aoi_report_functions import AOIReportFunctions
from vcis.reporting.pdf_reporting.prompt_properties import PromptProperties
from vcis.reporting.pdf_reporting.pdf_report_generator import ReportPDFGenerator

##############################################################################################################################
    # GEO Reporting Functions
##############################################################################################################################

class ReportGenerator():
    def __init__(self, geo_confidence_results=None, verbose=False):
        self.verbose = verbose
        self.properties = CDR_Properties()
        self.prompt_properties = PromptProperties()
        self.utils = CDR_Utils(verbose=self.verbose)
        self.cassandra_tools = CassandraTools()
        self.cassandra_spark_tools = CassandraSparkTools()
        self.oracle_tools = OracleTools()
        self.reporting_tools = ReportingTools(self.verbose)
        self.cotraveler_report_generator = CotravelerReportGenerator(self.verbose)
        self.cdr_report_generator = CdrReportGenerator()

        self.simulation_report_generator = SimulationReportGenerator()
        self.simulation_report_functions = SimulationReportFunctions()
        self.geolocation_analyzer : GeoLocationAnalyzer

        self.geo_confidence_results = geo_confidence_results
        self.blocks = []
        self.blocks_dictionary = {}
        self.blocks_aoi = []
        
    def get_geo_report(self, dataframe:pd.DataFrame=None ,df_common:pd.DataFrame=None,df_main:pd.DataFrame=None, table:pd.DataFrame=None, df_history:pd.DataFrame=None, report_type:str=None, file_name:str=None, table_id:str=None,distance:int = 50):
        device_id_main = df_main['device_id'].iloc[0]
        block_list = []
        kpis_blocks,dict_blocks = self.cotraveler_report_generator.get_cotraveler_kpis(table,device_id_main)
        block_list.extend(dict_blocks)
        try:
            common_countries_map = self.cotraveler_report_generator.get_common_countires_timeline(table)
        except:
            common_countries_map = ["Not Available" for _ in range(len(table))]
        self.blocks.append(common_countries_map) 
        # try:
        # time_spent = self.cotraveler_report_generator.get_time_spent(table,device_id_main)
        # block_list.extend(time_spent)

        # except:
        #     time_spent = ["Not Available" for _ in range(len(table))]
   
        # figure_time_spent_main_list, figure_time_spent_cotraveler_list= self.cotraveler_report_generator.get_time_spent_percentage(df = dataframe,df_main = df_main,table = table,df_history = df_history)
        
        # self.blocks.append(figure_time_spent_main_list)
        
        # self.blocks.append(figure_time_spent_cotraveler_list)
        try:
            figure_Mapping_Shared_Instances_Across_Time_list  = self.cotraveler_report_generator.get_Mapping_Shared_Instances_Across_Time(dataframe,table,distance)
        except:
            figure_Mapping_Shared_Instances_Across_Time_list = ["Not Available" for _ in range(len(table))]
        self.blocks.append(figure_Mapping_Shared_Instances_Across_Time_list)

        common_areas_plot,dict_blocks = self.cotraveler_report_generator.get_common_areas_plot(device_id_main= device_id_main,df_main = df_main,df_common = df_common,df_history=df_history,table=table)
        self.blocks.append(common_areas_plot)
        block_list.extend(dict_blocks)

        try:
            hit_distirbutions_graph_week,dict_blocks = self.cotraveler_report_generator.get_hit_distirbutions_week(device_id_main = device_id_main,df_main = df_main,df_common = df_common,table=table)
        except:
            hit_distirbutions_graph_week = ["Not Available" for _ in range(len(table))]
        self.blocks.append(hit_distirbutions_graph_week)
        block_list.extend(dict_blocks)
        try:
            hit_distirbutions_graph_month,dict_blocks = self.cotraveler_report_generator.get_hit_distirbutions_graph_month(device_id_main = device_id_main,df_main = df_main,df_common = df_common,table=table)
        except:
            hit_distirbutions_graph_month = ["Not Available" for _ in range(len(table))]
        
        block_list.extend(dict_blocks)
        self.reporting_tools.insert_report_blocks_into_database(block_list, table_id)


        self.blocks.append(hit_distirbutions_graph_month)
        
        self.blocks = [[sublist[i] for sublist in self.blocks] for i in range(len(self.blocks[0]))]

        report = self.reporting_tools.get_report_layout(self.blocks,kpis_blocks,table,device_id_main,report_type)
        
        self.reporting_tools.save_report(report = report ,report_path = self.properties.passed_filepath_reports_html,report_name = self.properties.passed_report_name_html_geo,open=True)
        
        script_contents = self.reporting_tools.get_head_scripts(self.properties.passed_filepath_reports_html+self.properties.passed_report_name_html_geo+'.html')
        self.oracle_tools.drop_create_table(table_name = self.properties.oracle_analytcis_report_table ,table_id = table_id , table_schema_query = self.properties._oracle_table_schema_query_analytics_report) 
        self.reporting_tools.insert_into_database(file_name , script_contents,table_id)
        
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : Mapping Shared Instances Across Time Report Complete'))

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
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★'.format('Reporting : CDR Report Complete'))

    def get_geo_confidence_report(self, aoi_blocks, table_id):
        self.reporting_tools.geo_confidence_flag = True
        new_block_group = []
        geo_confidence_report = self.geo_confidence_results

        for device_dict in aoi_blocks:

            for device_id, previous_block in device_dict.items():

                device_confidence_report = geo_confidence_report[geo_confidence_report['DEVICE_ID_GEO'] == device_id]

                # Get geospatial confidence block for specific device
                device_confidence_block = self.reporting_tools.get_geo_confidence_layout(device_confidence_report,table_id)

                new_block_list = [*device_confidence_block, *previous_block]

                group = dp.Group(blocks = new_block_list, label = device_id)

                aoi_block_dict = {device_id: group}
                new_block_group.append(aoi_block_dict)

        return new_block_group

    def get_aoi_prompt(self, prompt, aoi_report : AOIReportFunctions):
        device_confidence_report = self.geo_confidence_results[self.geo_confidence_results['DEVICE_ID_GEO'] == aoi_report.geolocation_data.device_id_geo]
        print(self.geo_confidence_results)

        device_id = device_confidence_report['DEVICE_ID_GEO'].values[0]
        start_date = device_confidence_report['START_DATE'].values[0]
        end_date = device_confidence_report['END_DATE'].values[0]
        total_days = device_confidence_report['RECORDED_DAYS'].values[0]
        total_hits = device_confidence_report['RECORDED_HITS'].values[0]

        confidence_score = device_confidence_report['CONFIDENCE_SCORE'].values[0]
        time_period_consistency = device_confidence_report['TIME_PERIOD_CONSISTENCY'].values[0]
        day_of_week_consistency = device_confidence_report['DAY_OF_WEEK_CONSISTENCY'].values[0]
        hits_per_day_consistency = device_confidence_report['HITS_PER_DAY_CONSISTENCY'].values[0]
        hour_of_day_consistency = device_confidence_report['HOUR_OF_DAY_CONSISTENCY'].values[0]

        prompt["geo_spatial"]["user"] = f"device_id : {device_id}, start_date : {start_date}, end_date : {end_date}, total_days : {total_days}, total_hits : {total_hits}"
        prompt["confidence_score"]["user"] = f"confidence_score : {confidence_score}"
        prompt["consistency_metrics"]["user"] = f"time_period_consistency : {time_period_consistency}, day_of_week_consistency : {day_of_week_consistency}, hits_per_day_consistency : {hits_per_day_consistency}, hour_of_day_consistency : {hour_of_day_consistency}"

        return prompt

    def get_aoi_report(self, geolocation_analyzer, table_id):
        self.reporting_tools.aoi_flag = True

        aoi_blocks = []
        device_report_items_dictionary = {}

        # Using a list of prompt dictionary, each element corresponds to a device
        prompt_dict = []

        for device in range(geolocation_analyzer.geolocation_data_list.get_length()):
            geolocation_data = geolocation_analyzer.geolocation_data_list[device]
            aoi_report = AOIReportFunctions()
            aoi_report.initialize_aoi_report(geolocation_data)

            blocks, report_items_dictionary = self.reporting_tools.get_report_layout_aoi(aoi_report,geolocation_data.device_id_geo,table_id)
            # Save the report items for each device in a dictionary
            device_report_items_dictionary[aoi_report.geolocation_data.device_id_geo] = report_items_dictionary

            blocks_list = {aoi_report.geolocation_data.device_id_geo : blocks}

            aoi_blocks.append(blocks_list)

            # Addition of a prompt dictionary list in order to be used in the pdf generator
            prompt = aoi_report.prompt_properties.prompt

            # Returns an edited version of the prompt dictionary depending on the results stored in the aoi_report object
            prompt = self.get_aoi_prompt(prompt, aoi_report)

            prompt_dict.append({device: prompt})

        # pdg_report = ReportPDFGenerator('pdf_report.pdf')


        # pdg_report.generate_pdf(prompt_dict)
        # Merge AOI Report with Geospatial Confidence Report
        new_block_group = self.get_geo_confidence_report(aoi_blocks,table_id)
        aoi_report = self.reporting_tools.merge_reports_to_select_dropdown(new_block_group)

        # Add a new tab on information depending on the content of new_block_list
        information_page = self.reporting_tools.get_information_page()

        # Merge both aoi_report and information_page in the same report
        final_report = dp.Report(
            dp.Page(aoi_report, title = "AOI Report"),
            dp.Page(information_page, title = "Information Page")
        )
        # final_report = dp.Report(
        #     dp.Page(aoi_report, title = "AOI Report"),
        #     dp.Page(information_page, title = "Information Page")
        # )

        self.reporting_tools.save_report(report = final_report, report_path = self.properties.passed_filepath_reports_html, 
                                        report_name = self.properties.passed_report_name_html_aoi,
                                        open=True)

        script_contents = self.reporting_tools.get_head_scripts(self.properties.passed_filepath_reports_html + self.properties.passed_report_name_html_aoi + ".html")

        self.oracle_tools.drop_create_table(table_name = self.properties.oracle_analytcis_report_table, table_id = table_id , table_schema_query = self.properties._oracle_table_schema_query_analytics_report)
        
        self.reporting_tools.insert_into_database('report' , script_contents, table_id)

        return device_report_items_dictionary
    

    def get_simulation_report(self, main_table:pd.DataFrame=None, report_table:pd.DataFrame=None, report_type:int=None, file_name:str=None ,table_id:int = None,report_name:str=None , description:str=None):
        
        # try:
        #     self.oracle_tools.drop_create_table(table_name = self.properties.oracle_analytcis_simulation_blocks_table, table_id = table_id , table_schema_query = self.properties._oracle_table_schema_query_analytics_report_blocks,drop=False)
        # except:
        #     print("Table already exists")

        # Initialize the block list
        block_list = []

        main_map = self.simulation_report_generator.get_main_map(report_table, table_id)
        self.blocks_dictionary["main_map"] = main_map
        
        # confidence_analysis_df = self.simulation_report_generator.get_confidence_analysis(report_table,table_id)
        # self.blocks_dictionary["confidence_analysis_df"] = confidence_analysis_df
        # kpi_test = """
        #     <table style='width: 300px; height: 150px; border: 1px solid; 
        #             border-color: #d3d3d3; border-radius: 15px 15px 15px 15px; 
        #             background: linear-gradient(90deg, rgba(0,33,71,1) 0%, rgba(9,9,121,1) 49%, rgba(0,65,255,1) 112.1%);'>
        #     <tbody>
        #     <tr>
        #     <td style='width: 10%;'></td>
        #     <td style='width: 90%; color: black'>Profit &emsp; <b><span style='font-size:30px'>"&_cur_profit&"</b></td>
        #     </tr>
        #     <tr>
        #     <td style='width: 10%;'></td>
        #     <td style='width: 90%'>"&_f_change&" <b><span style='font-size:14px; color: black'>"&_change&"</b><span style='font-size:12px; color: black'> &nbsp;  vs last month</td>
        #     </tr>
        #     </tbody>
        #     </table>
        # """
        # self.blocks_dictionary["kpi"] = kpi_test
        
        number_of_hits_server_providers = self.simulation_report_generator.get_number_of_hits_per_server_provider(report_table)
        self.blocks_dictionary["number_of_hits_server_providers"] = number_of_hits_server_providers['BLOCK_CONTENT']
        block_list.extend([number_of_hits_server_providers])

        report_table = self.simulation_report_functions.process_data(report_table,report_type)
        number_of_hits_per_dow = self.simulation_report_generator.get_number_of_hits_per_dow(report_table)
        self.blocks_dictionary["number_of_hits_per_dow"] = number_of_hits_per_dow['BLOCK_CONTENT']
        block_list.extend([number_of_hits_per_dow])

        number_of_hits_per_month = self.simulation_report_generator.get_number_of_hits_per_month(report_table)
        self.blocks_dictionary["number_of_hits_per_month"] = number_of_hits_per_month['BLOCK_CONTENT']
        block_list.extend([number_of_hits_per_month])

        number_of_hits_per_hod = self.simulation_report_generator.get_number_of_hits_per_hod(report_table)
        self.blocks_dictionary["number_of_hits_per_hod"] = number_of_hits_per_hod['BLOCK_CONTENT']
        block_list.extend([number_of_hits_per_hod])

        summary_statistics = self.simulation_report_generator.get_summary_statistics(report_table,report_type)
        self.blocks_dictionary["summary_statistics"] = summary_statistics['BLOCK_CONTENT']
        block_list.extend([summary_statistics])

        if report_type==11:
            number_of_hits_per_cgi = self.simulation_report_generator.get_number_of_hits_per_cgi(report_table)
            self.blocks.append(number_of_hits_per_cgi)

        timeline_funtions = TimeLineFunctions()
        timeline_figure = timeline_funtions.timeline(table_id=table_id)
        timeline_figure = timeline_figure + """<script type="text/javascript" src="https://cdn.bokeh.org/bokeh/release/bokeh-3.4.1.min.js"></script>
            <script type="text/javascript" src="https://cdn.bokeh.org/bokeh/release/bokeh-widgets-3.4.1.min.js"></script>"""
        
        timeline_figure_block = {
            'BLOCK_CONTENT': timeline_figure,
            'BLOCK_NAME': 'Timeline',
            'BLOCK_TYPE': 'HTML',
            'BLOCK_ASSOCIATION': 'Simulation',
            'DEVICE_ID': 'id'
        }
        self.blocks_dictionary["timeline"] = timeline_figure
        block_list.extend([timeline_figure_block])

        # Save simulation report components in database
        self.reporting_tools.insert_report_blocks_into_database(block_list, table_id = table_id)

        report, report_intoduction = self.reporting_tools.get_simulation_report_layout(main_table = main_table,report_table = report_table,report_name=report_name,report_type=report_type ,blocks=self.blocks_dictionary,description=description,table_id=table_id)

        self.reporting_tools.save_report(report = report ,report_path = self.properties.passed_filepath_reports_html,open=True,report_name=self.properties.passed_report_name_html_simulation)
        
        script_contents = self.reporting_tools.get_head_scripts(self.properties.passed_filepath_reports_html+self.properties.passed_report_name_html_simulation+'.html')
        
        self.oracle_tools.drop_create_table(table_name = self.properties.oracle_analytcis_report_table ,table_id = table_id , table_schema_query = self.properties._oracle_table_schema_query_analytics_report)
         
        self.reporting_tools.insert_into_database(file_name, script_contents, table_id)
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Rporting : Simulation Report Complete'))


        return report_intoduction