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

from vcis.utils.properties_ai import AIProperties
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.reporting_pdf.simulation_reporting.simulation_report_functions import SimulationReportFunctions
from vcis.reporting_pdf.simulation_reporting.simulation_report_layout import SimulationReportLayout
from vcis.reporting_pdf.simulation_reporting.simulation_report_layout_html import SimulationReportLayoutHTML
from vcis.ai.models.groq.model import GroqAIModel
from vcis.reporting_pdf.report_main_pdf.pdf_reporting_tools import ReportingTools
from vcis.reporting.report_main.reporting_tools import ReportingTools as ReportingToolsHTML
from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.ai.tools.vcis_tools import vcisTools
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.plots.plots_tools import PlotsTools

class SimulationReportGenerator():
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.properties = CDR_Properties()
        self.utils = CDR_Utils()
        self.cassandra_tools = CassandraTools()
        self.simulation_report_functions = SimulationReportFunctions()
        self.simulation_report_layout = SimulationReportLayout()
        self.model = GroqAIModel()
        self.properties_ai = AIProperties()
        self.reporting_tools = ReportingTools()
        self.reporting_tools_html = ReportingToolsHTML()
        self.simulation_report_layout_html = SimulationReportLayoutHTML()
        self.vcis_tools = vcisTools()
        self.oracle_tools = OracleTools()
        self.plot_tools = PlotsTools()
        self.dh_dictionary = {}

    def get_simulation_report_dh(self,table_id:int=None , table:pd.DataFrame= None, start_date:str=None, end_date:str=None):
        title = "Targeted Device Behavior Analysis"
        self.dh_dictionary['title'] = title
        self.dh_dictionary['table_id'] = table_id

        original_table, table , mapping_table = self.simulation_report_functions.get_simulation_data(table_id=table_id , table=table)
        self.dh_dictionary['mapping_table'] = mapping_table
        device_id_list = original_table['device_id'].unique().tolist()
        shapes = self.oracle_tools.get_simulation_shapes(table_id=table_id)
        main_map = self.plot_tools.get_simulation_plot(data=table, shapes = shapes)

        self.dh_dictionary['main_map'] = main_map

        summary_table,messages = self.simulation_report_functions.get_summary_table(table=table)
        introduction = self.simulation_report_functions.get_report_introduction(table=table)
        start_date = start_date.date()
        end_date = end_date.date()
        print(start_date)
        start_date = self.utils.convert_datetime_to_ms_str(str(start_date))
        end_date = self.utils.convert_datetime_to_ms_str(str(end_date))
        server = '10.1.10.110'
        device_report_items_dictionary , cotraveler_table ,df_merged_list, df_device,df_common, df_history, cotraveler_distance, top_id, heatmap_plots, cotraveler_barchart , correlation_description, cotraveler_user_prompt = self.vcis_tools.get_everything(device_id_list=device_id_list,
                                                                                                                                                                                                                                                                    start_date=start_date,
                                                                                                                                                                                                                                                                    end_date=end_date,
                                                                                                                                                                                                                                                                    table_id=table_id,
                                                                                                                                                                                                                                                                    server=server)
        self.dh_dictionary['heatmap_plots'] = heatmap_plots
        self.dh_dictionary['cotraveler_barchart'] = cotraveler_barchart

        main_device_dictionary = device_report_items_dictionary[f'{device_id_list[0]}']
        aoi_table = main_device_dictionary['AOI Table'][0]
        aoi_mapp = main_device_dictionary['AOI Map'][0]
        suspiciousness_results = main_device_dictionary['Suspiciousness Results'][0]
        location_likelihood_wrt_dow_fig = main_device_dictionary['Location Likelihood wrt Dow'][0]
        location_likelihood_wrt_dow_description = main_device_dictionary['Location Likelihood wrt Dow'][1]
        location_duration_wrt_dow_fig = main_device_dictionary['Location Duration wrt Dow'][0]
        location_duration_wrt_dow_description = main_device_dictionary['Location Duration wrt Dow'][1]

        self.dh_dictionary['aoi_table'] = aoi_table
        self.dh_dictionary['aoi_mapp'] = aoi_mapp
        self.dh_dictionary['location_likelihood_wrt_dow_fig'] = location_likelihood_wrt_dow_fig
        self.dh_dictionary['location_duration_wrt_dow_fig'] = location_duration_wrt_dow_fig
        self.dh_dictionary['location_duration_wrt_dow_description'] = location_duration_wrt_dow_description

        home_location_df = aoi_table[aoi_table['AOI TYPE']=='Home']
        home_location = home_location_df['LOCATION'].values[0]
        home_location_coords = f"({home_location_df['LATITUDE'].values[0]},{home_location_df['LONGITUDE'].values[0]})"


        if len(home_location_df)>0:
            new_rows = {
                    'Statistic': ['Address', 'Address Location'],
                    'Data': [home_location,home_location_coords]
                }
            df_new_rows = pd.DataFrame(new_rows)
            summary_table = summary_table.append(df_new_rows, ignore_index=True)
        
        if len(top_id)>0:
            new_rows = {
                    'Statistic': ['IMSI', 'IMEI', 'Phone Number','First Activity Recorded'],
                    'Data': [top_id['imsi_id'][0],top_id['imei_id'][0],f"+{top_id['phone_number'][0]}", self.utils.convert_ms_to_datetime_value(top_id['min'][0])]
                }
            df_new_rows = pd.DataFrame(new_rows)
            summary_table = summary_table.append(df_new_rows, ignore_index=True)
        self.dh_dictionary['summary_table'] = summary_table

        print(cotraveler_table)
        cotraveler_table = cotraveler_table[['RANK', 'DEVICE_ID', 'TOTAL_HITS', 'COUNT',
            'SEQUENCE_DISTANCE', 'LONGEST_SEQUENCE', 'GRID_SIMILARITY',
            'TOTAL_MERGED_HITS', 'UNCOMMON_HITS_RATIO',
            'TOTAL_UNCOMMMON_TIME_SPENT', 'AVERAGE_UNCOMMMON_TIME_SPENT',
            'CLASSIFICATION']]
        
        self.dh_dictionary['cotraveler_table'] = cotraveler_table

        city_country_movements , fig_devices_movments, threat_score_table, threat_score_description = self.simulation_report_functions.get_devices_multi_country_city_movement(table=table)
        
        self.dh_dictionary['fig_devices_movments'] = fig_devices_movments
        self.dh_dictionary['threat_score_table'] = threat_score_table

        aoi_description=''
        for index, row in aoi_table.iterrows():
            if row['AOI TYPE'] =='Home':

                # Format the information into a string
                description = f"device_id Home address is :{row['LOCATION']} at this location: ({row['LATITUDE']},{row['LONGITUDE']}) spending {row['TOTAL DAYS']} Days"
                aoi_description+=description
        print(aoi_description)
        # common_locations, common_location_fig, common_location_map = self.simulation_report_functions.get_common_locations(table=table)
        user_prompt = f"""

            Introduction : {introduction}

            Summary :{messages}

            Devices Movement: {city_country_movements}

            Threat Description:{threat_score_description}

            Correlations: {correlation_description}

            Home Address: {aoi_description}

            """
        
        # llm = self.model.get_model(model_name = "llama3-8b-8192",local=False)
        llm = self.model.get_model(local=False)

        ai_response = self.model.get_chain(llm=llm,system_prompt=self.properties_ai.system_prompt_simulation_dh,user_prompt=user_prompt)
        
        abstract_start_marker = '**Abstract**'
        abstract_end_marker = '**Introduction**'
        abstract = self.reporting_tools.get_exctract_content(ai_response,start_marker=abstract_start_marker,end_marker=abstract_end_marker)
        self.dh_dictionary['abstract'] = abstract

        introduction_start_marker = "**Introduction**"
        introduction_end_marker = "**Description and Analysis of Device Movement**"
        introduction = self.reporting_tools.get_exctract_content(ai_response,start_marker=introduction_start_marker,end_marker=introduction_end_marker)
        self.dh_dictionary['introduction'] = introduction

        movement_analysis_start_marker = "**Description and Analysis of Device Movement**"
        movement_analysis_end_marker = "**Threat Analysis**"
        movement_analysis = self.reporting_tools.get_exctract_content(ai_response,start_marker=movement_analysis_start_marker,end_marker=movement_analysis_end_marker)
        self.dh_dictionary['movement_analysis'] = movement_analysis

        threat_analysis_start_marker = "**Threat Analysis**"
        threat_analysis_end_marker = "**Patterns and Links**"
        threat_analysis = self.reporting_tools.get_exctract_content(ai_response,start_marker=threat_analysis_start_marker,end_marker=threat_analysis_end_marker)
        self.dh_dictionary['threat_analysis'] = threat_analysis

        patterns_links_marker = "**Patterns and Links**"
        patterns_links_end_marker = "**Conclusion**"
        patterns_links = self.reporting_tools.get_exctract_content(ai_response,start_marker=patterns_links_marker, end_marker=patterns_links_end_marker)
        self.dh_dictionary['patterns_links'] = patterns_links

        conclusion_start_marker = "**Conclusion**"
        conclusion = self.reporting_tools.get_exctract_content(ai_response,start_marker=conclusion_start_marker)
        self.dh_dictionary['conclusion'] = conclusion

        location_likelihood_wrt_dow_description_string = ''
        for description in location_likelihood_wrt_dow_description:
            location_likelihood_wrt_dow_description_string+=description + '\n'
        ai_response = self.model.get_chain(llm=llm,system_prompt=self.properties_ai.prompt_system_prompt_location_likelihood,user_prompt=location_likelihood_wrt_dow_description_string)
        # print(ai_response)

        location_likelihood_Observation_marker_start_marker = "**Observation**"
        location_likelihood_Observation_marker_end_marker = "**Analysis**"

        location_likelihood_Observation = self.reporting_tools.get_exctract_content(ai_response,start_marker=location_likelihood_Observation_marker_start_marker,end_marker=location_likelihood_Observation_marker_end_marker)
        self.dh_dictionary['location_likelihood_Observation'] = location_likelihood_Observation



        location_likelihood_analysis_marker_start_marker = "**Analysis**"

        location_likelihood_analysis = self.reporting_tools.get_exctract_content(ai_response,start_marker=location_likelihood_analysis_marker_start_marker)

        self.dh_dictionary['location_likelihood_analysis'] = location_likelihood_analysis

        location_duration_wrt_dow_description_string = ''
        for description in location_duration_wrt_dow_description:
            location_duration_wrt_dow_description_string+=description + '\n'
        ai_response = self.model.get_chain(llm=llm,system_prompt=self.properties_ai.prompt_system_prompt_location_duration,user_prompt=location_duration_wrt_dow_description)
        print(ai_response)
        
        location_likelihood_duration_Observation_marker_start_marker = "**Observation**"
        location_likelihood_duration_Observation_marker_end_marker = "**Analysis**"

        location_likelihood_duration_Observation = self.reporting_tools.get_exctract_content(ai_response,start_marker=location_likelihood_duration_Observation_marker_start_marker,end_marker=location_likelihood_duration_Observation_marker_end_marker)
        self.dh_dictionary['location_likelihood_duration_Observation'] = location_likelihood_duration_Observation



        location_likelihood_duration_analysis_marker_start_marker = "**Analysis**"

        location_likelihood_duration_analysis = self.reporting_tools.get_exctract_content(ai_response,start_marker=location_likelihood_duration_analysis_marker_start_marker)

        self.dh_dictionary['location_likelihood_duration_analysis'] = location_likelihood_duration_analysis
        # device_colocation_start_marker = "**Device Co-location Analysis**"

        # device_colocation_end_marker = "**Significance of Locations**"

        # device_colocation = self.reporting_tools.get_exctract_content(ai_response,start_marker=device_colocation_start_marker,end_marker=device_colocation_end_marker)

        # significane_location_start_marker = "**Significance of Locations**"

        # significane_location =  self.reporting_tools.get_exctract_content(ai_response,start_marker=significane_location_start_marker)

        ai_response = self.model.get_chain(llm=llm,system_prompt=self.properties_ai.system_prompt_cotraveler,user_prompt=cotraveler_user_prompt)

        print(ai_response)

        cotraveler_barchart_dow_desctription_start_marker = "**Analysis**"
        cotraveler_barchart_dow_desctription_end_marker = "**Plot Analysis**"

        cotraveler_barchart_dow_desctription = self.reporting_tools.get_exctract_content(ai_response,start_marker=cotraveler_barchart_dow_desctription_start_marker,end_marker=cotraveler_barchart_dow_desctription_end_marker)
        self.dh_dictionary['cotraveler_barchart_dow_desctription'] = cotraveler_barchart_dow_desctription

        cotraveler_barchart_analysis_start_marker = "**Plot Analysis**"
        cotraveler_barchart_analysis_end_marker = "**Conclusion**"

        cotraveler_barchart_analysis = self.reporting_tools.get_exctract_content(ai_response,start_marker=cotraveler_barchart_analysis_start_marker,end_marker=cotraveler_barchart_analysis_end_marker)

        self.dh_dictionary['cotraveler_barchart_analysis'] = cotraveler_barchart_analysis

        # self.simulation_report_layout.generate_pdf_dh(title= title,
        #                                            table_id = table_id,
        #                                            abstract = abstract,
        #                                            introduction = introduction,
        #                                            summary_table =summary_table,
        #                                            movement_analysis = movement_analysis,
        #                                            patterns_links = patterns_links,
        #                                            common_location = common_location,
        #                                            device_colocation = device_colocation, 
        #                                            significane_location = significane_location,
        #                                            conclusion = conclustion,
        #                                            mapping_table = mapping_table)

        report  = self.simulation_report_layout_html.get_simulation_report_layout_dh(self.dh_dictionary)

        self.reporting_tools_html.save_report(report = report ,report_path = self.properties.passed_filepath_reports_html,open=True,report_name=self.properties.passed_report_name_html_simulation_ai)


        
    def get_simulation_report_ac(self,table_id:int=None , table:pd.DataFrame=None):



        title = "Simulation Report"
        original_table ,table ,mapping_table= self.simulation_report_functions.get_simulation_data(table_id=table_id , table=table)
        summary_table,messages = self.simulation_report_functions.get_summary_table(table=table)
        introduction, start_date, end_date = self.simulation_report_functions.get_report_introduction(table=table)
        dow_content , dow_fig = self.simulation_report_functions.get_number_of_hits_per_dow(data=table)
        time_spent ,time_spent_list_description = self.simulation_report_functions.get_time_spent(table = table)
        hourly_activity_df , hourly_active_devices ,hourly_activty_fig = self.simulation_report_functions.get_hourly_active_device(table = table)
        geofencing = self.simulation_report_functions.get_geofencing(table=original_table,start_date=start_date,end_date=end_date)
        
        user_prompt = f"""

            Introduction : {introduction}

            Summary :{messages}

            Devices Movement: {dow_content}

            """
        llm = self.model.get_model(local=False)

        ai_response = self.model.get_chain(llm=llm,system_prompt=self.properties_ai.system_prompt_simulation_ac,user_prompt=user_prompt)

        abstract_start_marker = '**Abstract**'
        abstract_end_marker = '**Introduction**'
        abstract = self.reporting_tools.get_exctract_content(ai_response,start_marker=abstract_start_marker,end_marker=abstract_end_marker)
        
        introduction_start_marker = "**Introduction**"
        introduction_end_marker = "**Description and Analysis of The Activity Scan Hits Distribution**"
        introduction = self.reporting_tools.get_exctract_content(ai_response,start_marker=introduction_start_marker,end_marker=introduction_end_marker)
        
        movement_analysis_start_marker = "**Description and Analysis of The Activity Scan Hits Distribution**"
        movement_analysis_end_marker = "**Conclusion**"
        activity_scan_hits_distribution = self.reporting_tools.get_exctract_content(ai_response,start_marker=movement_analysis_start_marker,end_marker=movement_analysis_end_marker)

        conclusion_start_marker = "**Conclusion**"
        conclustion = self.reporting_tools.get_exctract_content(ai_response,start_marker=conclusion_start_marker)
        user_prompt = f"""

            Time Spent Description : {time_spent_list_description}

            """
        ai_response = self.model.get_chain(llm=llm,system_prompt=self.properties_ai.system_prompt_time_spent,user_prompt=user_prompt)
        
        timespent_analysis_start_marker = '**TimeSpent Analysis at AOI**'
        timespent_analysis_end_marker = '**Insights and Observations**'
        user_prompt = f"""

            Time Spent Description : {hourly_active_devices}

            """
        timespent_analysis = self.reporting_tools.get_exctract_content(ai_response,start_marker=timespent_analysis_start_marker,end_marker=timespent_analysis_end_marker)
        
        timespent_insights_observation_start_marker = '**Insights and Observations**'
        timespent_insights_observation = self.reporting_tools.get_exctract_content(ai_response,start_marker=timespent_insights_observation_start_marker)
        ai_response = self.model.get_chain(llm=llm,system_prompt=self.properties_ai.system_prompt_hourly_activity,user_prompt=user_prompt)
        
        hourly_activity_start_marker = '**Hourly Device Activity at AOI**'
        hourly_activity_end_marker = '**Most Active Devices by Hour**'
        hourly_activity = self.reporting_tools.get_exctract_content(ai_response,start_marker=hourly_activity_start_marker,end_marker=hourly_activity_end_marker)
        
        most_active_devices_start_marker = '**Most Active Devices by Hour**'
        most_active_devices_end_marker = '**Device Activity Patterns**'
        most_active_devices = self.reporting_tools.get_exctract_content(ai_response,start_marker=most_active_devices_start_marker,end_marker=most_active_devices_end_marker)
        
        device_activity_patterns_start_marker = '**Device Activity Patterns**'
        device_activity_patterns = self.reporting_tools.get_exctract_content(ai_response,start_marker=device_activity_patterns_start_marker)
        
        print(ai_response)


        # self.simulation_report_layout.generate_pdf_ac(title= title,
        #                                                 table_id = table_id,
        #                                                 abstract = abstract,
        #                                                 introduction = introduction,
        #                                                 summary_table =summary_table,
        #                                                 activity_scan_hits_distribution = activity_scan_hits_distribution,
        #                                                 timespent_analysis = timespent_analysis,
        #                                                 timespent_insights_observation = timespent_insights_observation,
        #                                                 conclusion = conclustion,
        #                                                 mapping_table = mapping_table)
        # print(ai_response)


        report  = self.simulation_report_layout_html.get_simulation_report_layout_ac(title= title,
                                                                            table_id = table_id,
                                                                            abstract = abstract,
                                                                            introduction = introduction,
                                                                            summary_table =summary_table,
                                                                            dow_fig = dow_fig,
                                                                            activity_scan_hits_distribution = activity_scan_hits_distribution,
                                                                            timespent_analysis = timespent_analysis,
                                                                            timespent_insights_observation = timespent_insights_observation,
                                                                            hourly_activity = hourly_activity,
                                                                            most_active_devices = most_active_devices,
                                                                            device_activity_patterns = device_activity_patterns,
                                                                            hourly_activty_fig = hourly_activty_fig,
                                                                            hourly_activity_df = hourly_activity_df,
                                                                            conclusion = conclustion,
                                                                            mapping_table = mapping_table)
        

        self.reporting_tools_html.save_report(report = report ,report_path = self.properties.passed_filepath_reports_html,open=True,report_name=self.properties.passed_report_name_html_simulation_ai)

