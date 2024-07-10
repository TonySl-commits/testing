
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
import base64
import json
import country_converter as coco

from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.reporting.geo_reporting.geo_confidence_report.geo_confidence_report_functions import GeoConfidenceReportFunctions
from vcis.reporting.geo_reporting.aoi_report.aoi_report_functions import AOIReportFunctions

class ReportingTools:
    def __init__(self, verbose=False):
        self.properties = CDR_Properties()
        self.utils = CDR_Utils(verbose=True)
        self.cassandra_tools = CassandraTools()
        self.cassandra_spark_tools = CassandraSparkTools()
        self.geo_confidence_report = GeoConfidenceReportFunctions()
        self.oracle_tools = OracleTools()
        self.verbose = verbose
        self.geo_confidence_flag = False
        self.aoi_flag = False
        self.cdr_flag = False
        self.cotraveler_flag = False
        self.geo_confidence_flag = False
        self.aoi_flag = False
        self.cdr_flag = False
        self.cotraveler_flag = False

##############################################################################################################################
    # Reportin Functions
##############################################################################################################################
    # def format_text(self,title:str=None, text:str=None , header:int=1):
    #     # Define the CSS styles
    #     css_style = """
    #     <style>
    #     .custom-text {
    #             color: #002147;
    #             font-family: 'Times New Roman', Times, serif;
    #             text-align: justify;
    #         }
    #     </style>
    #     """

    #     # Create the HTML structure
    #     html_content = f"""
    #     {css_style}
    #     <div class="custom-text">
    #         <h{header}>{title}</h{header}>
    #         <p>{text}</p>
    #     </div>
    #     """

    #     return html_content

    def format_text(self, title: str = None, text: str = None, header: int = 1, include_title: bool = True):
        # Define the CSS styles
        css_style = """
        <style>
        .custom-text {
            color: #002147;
            font-family: 'Times New Roman', Times, serif;
            text-align: justify;
            font-size: 18px; /* Increase font size */
            padding: 0px; /* Decrease padding */
        }
        </style>
        """

        # Start building the HTML structure
        html_content = css_style

        if include_title:
            # Conditionally include the title based on the include_title flag
            html_content += f"<div class=\"custom-text\">\n"
            html_content += f"    <h{header}>{title}</h{header}>\n"
            html_content += "    <p>"
            
        else:
            # Directly start with the paragraph tag if title is not included
            html_content += "<div class=\"custom-text\">\n"
            html_content += "    <p>"
        
        # Always include the text
        html_content += text + "</p>\n</div>"

        return html_content
    # Common Functions

    def get_report_blocks(self,blocks_list:list,report_type:str=None):
        blocks = []
        # for fig in fig_list:
        #     blocks.append(dp.Plot(fig))
        if blocks_list[0]!="Not Available":
            print(blocks_list[0])
            blocks.append(dp.Plot(blocks_list[0]))
        else:
            blocks.append(dp.HTML(blocks_list[0]))

        # blocks.append(dp.Group(blocks = [dp.Plot(blocks_list[1]),dp.Plot(blocks_list[2])],columns=2))
        if blocks_list[1]!="Not Available":
            print("try2")
            blocks.append(dp.Plot(blocks_list[1]))
        else:
            print("except2")
            blocks.append(dp.HTML(blocks_list[1]))
        if blocks_list[2]!="Not Available":
            print("try3")
            blocks.append(dp.Plot(blocks_list[2]))
        else:
            blocks.append(dp.HTML(blocks_list[2]))
            print("except3")
        if blocks_list[3]!="Not Available":
            print("try4")
            blocks.append(dp.Plot(blocks_list[3]))
        else:
            print("except4")
            blocks.append(dp.HTML(blocks_list[3]))

        return blocks
    
    ##############################################################################################################################

    def get_report_blocks_cdr(self,blocks_list:list):
        blocks = [dp.Group(blocks = [dp.Plot(blocks_list[1]),dp.DataTable(blocks_list[2])],columns=2),dp.Plot(blocks_list[0])]
        return blocks
    
    ##############################################################################################################################

    def get_report_blocks_simulation(self,blocks_list,report_type:int):

        if report_type==11:
            blocks = [dp.Plot(blocks_list[0]),
                dp.Group(blocks = [dp.Plot(blocks_list[1]),
                            dp.Plot(blocks_list[2]),
                            dp.DataTable(blocks_list[5]),
                            dp.Plot(blocks_list[4])],
                            columns=2),
           dp.Group(blocks = [ dp.Plot(blocks_list[3]),dp.Plot(blocks_list[6])],columns=2)]
        else:
            main_map = dp.Plot(blocks_list['main_map'])
            smilation_table = dp.DataTable(blocks_list['summary_statistics'], label="Simulation Details")
            server_provider_piechart = dp.Plot(blocks_list['number_of_hits_server_providers'], label="Server Provider")
            # confidence_analysis_table = blocks_list['confidence_analysis_df']
            # confidence_analysis_table_list = []
            # for table in confidence_analysis_table:
            #     block = dp.DataTable(table)
            #     confidence_analysis_table_list.append(block)
            # confidence_analysis_select = dp.Select(blocks=confidence_analysis_table_list, label="Confidence Analysis",type=dp.SelectType.DROPDOWN)
            server_provider = dp.Group(blocks=[server_provider_piechart ], label="Server Provider")
            timeline = dp.HTML(blocks_list['timeline'], label="Timeline")
            behavioral_analysis = dp.Group(blocks=[dp.Group(blocks=[dp.Plot(blocks_list['number_of_hits_per_dow']), dp.Plot(blocks_list['number_of_hits_per_hod'])],columns=2),dp.Plot(blocks_list['number_of_hits_per_month'])],label='Behavioral Analysis')
            # blocks = [ dp.Group(blocks = [main_map]),
            #         dp.Select(blocks=[smilation_table ,server_provider, behavioral_analysis, timeline])]
            blocks = [smilation_table ,server_provider, behavioral_analysis, timeline]
            
        return blocks,main_map
    
    def get_report_blocks_aoi(self,blocks_list:list):
        blocks = []
        keys = list(blocks_list.keys())
        for key in keys:
            confidence_blocks = dp.Group(blocks = [dp.Plot(blocks_list[key]['gauge']),dp.Plot(blocks_list[key]['confidence_metrics'])],columns=2)
            confidence_description_blocks = dp.Group(blocks = [dp.HTML(self.format_text(text = blocks_list[key]['confidence_score_explanation'],include_title=False)),dp.HTML(self.format_text(text = blocks_list[key]['metrics_explanation'],include_title=False))],columns=2)

            number_of_hits_per_dow = dp.Group(blocks = [dp.Plot(blocks_list[key]['hits_per_dow']),dp.HTML(self.format_text(text = blocks_list[key]['hits_per_dow_description'],include_title=False))],label='Number of Hits Per Day of Week')
            number_of_hits_per_month = dp.Group(blocks = [dp.Plot(blocks_list[key]['hits_per_month']),dp.HTML(self.format_text(text = blocks_list[key]['hits_per_month_description'],include_title=False))],label='Number of Hits Per Month')
            number_of_hits_select = dp.Select(blocks=[number_of_hits_per_dow,number_of_hits_per_month], label="Number of Hits")

            likelihood_dow = dp.Group(blocks = [dp.Plot(blocks_list[key]['lilkelihood_dow']),dp.HTML(self.format_text(text = blocks_list[key]['lilkelihood_dow_description'],include_title=False))],label='Likelihood Per Day of Week')
            duration_dow = dp.Group(blocks = [dp.Plot(blocks_list[key]['duration_dow']),dp.HTML(self.format_text(text = blocks_list[key]['duration_dow_description'],include_title=False))],label='Duration Per Day of Week')
            likelihod_select = dp.Select(blocks=[likelihood_dow,duration_dow], label="Likelihood")

            block = dp.Group(blocks = [dp.Group(blocks = [confidence_blocks,confidence_description_blocks,number_of_hits_select,likelihod_select])],label = key)
            blocks.append(block)
        select = dp.Select(blocks=blocks, type=dp.SelectType.DROPDOWN, label="Select Device")
        aoi_block = dp.Group(blocks=[select],label="Area of Interest")
        return aoi_block
    
    def get_report_blocks_cotravelers(self,blocks_list:list):

        blocks = []
        for device_id in blocks_list:
            devices_group = []
            for key in blocks_list[device_id]:
                kpi_key_list = json.loads(blocks_list[device_id][key]['Cotraveler KPIs'])
                
                kpi =  dp.Group(
                dp.BigNumber(
                    heading="Classification",
                    value=f"{kpi_key_list[0]}"
                    ),
                dp.BigNumber(
                    heading="Common Instances Similarity",
                    value=f"{round(kpi_key_list[1],1)}%",
                    
                ),
                dp.BigNumber(
                    heading="Longest Common Squences",
                    value=f"{kpi_key_list[2]}",
                ),
                dp.BigNumber(
                    heading="AVG Sequence Distance",
                    value=f"{round(kpi_key_list[3],1)}",
                ),
                columns=4, label = key
                )

                common_area = dp.Plot(blocks_list[device_id][key]['Common Areas Plot'])
                number_of_hits_per_dow = dp.Plot(blocks_list[device_id][key]['Number of Hits Per Day of Week'],label='Number of Hits Per Day of Week')
                number_of_hits_per_month = dp.Plot(blocks_list[device_id][key]['Number of Hits Per Month'],label='Number of Hits Per Month')

                number_of_hits_group = dp.Select(blocks = [number_of_hits_per_dow,number_of_hits_per_month],label = 'Number of Hits')

                device_group = dp.Group(blocks = [kpi,common_area,number_of_hits_group],label = key)
                devices_group.append(device_group)

            kpi_select = dp.Select(blocks=devices_group, type=dp.SelectType.DROPDOWN, label="Cotravelrs")
            cotraveler_block = dp.Group(blocks=[kpi_select],label="Cotravelrs")
            # blocks.append(cotraveler_block)
        return cotraveler_block

    def get_report_selector_blocks(self,fig_list:list,kpis_blocks:list,table:pd.DataFrame=None,report_type:str=None):
        block_list = []
        unique_devices = table['DEVICE_ID']
        rank_list = table['RANK']
        i = 0
        for sub_fig_list in fig_list:
            blockss = self.get_report_blocks(sub_fig_list,report_type)
            # print(blockss)
            plots = dp.Group(blocks = blockss[:2])
            colocated_hits = dp.Select(
                dp.Group(blockss[2], label= 'Distribution of Colocated Hits per Week'),
                dp.Group(blockss[3], label= 'Distribution of Colocated Hits per Month')
            )
            kpi =  dp.Group(
                dp.BigNumber(
                    heading="Common Instances",
                    value=f"{kpis_blocks[i][0]}"
                    ),
                dp.BigNumber(
                    heading="Common Instances Similarity",
                    value=f"{round(kpis_blocks[i][1],1)}%",
                    
                ),
                dp.BigNumber(
                    heading="Longest Common Squences",
                    value=f"{kpis_blocks[i][2]}",
                ),
                dp.BigNumber(
                    heading="AVG Sequence Distance",
                    value=f"{round(kpis_blocks[i][3],1)}",
                ),
            columns=4
            )
            block_list.append(dp.Group(kpi,
                                    plots,
                                    colocated_hits,
                                    label=f'Rank {rank_list[i+1]}: {unique_devices[i+1]}'))
            i+=1
        return block_list
    
    ##############################################################################################################################
    
    def get_report_selector_blocks_aoi(self,fig_list:list,kpis_blocks:list,table:pd.DataFrame=None,report_type:str=None):
        block_list = []
        unique_devices = table['DEVICE_ID']
        rank_list = table['RANK']
        i = 0
        for sub_fig_list in fig_list:
            blockss = self.get_report_blocks(sub_fig_list,report_type)
            # print(blockss)
            plots = dp.Group(blocks = blockss[:3])
            colocated_hits = dp.Select(
                dp.Group(blockss[3], label= 'Distribution of Colocated Hits per Week'),
                dp.Group(blockss[4], label= 'Distribution of Colocated Hits per Month')
            )
            kpi =  dp.Group(
                dp.BigNumber(
                    heading="Common Instances",
                    value=f"{kpis_blocks[i][0]}"
                    ),
                dp.BigNumber(
                    heading="Common Instances Similarity",
                    value=f"{round(kpis_blocks[i][1],1)}%",
                    
                ),
                dp.BigNumber(
                    heading="Longest Common Squences",
                    value=f"{kpis_blocks[i][2]}",
                ),
                dp.BigNumber(
                    heading="AVG Sequence Distance",
                    value=f"{round(kpis_blocks[i][3],1)}",
                ),
            columns=2
            )
            block_list.append(dp.Group(kpi,
                                    plots,
                                    colocated_hits,
                                    label=f'Rank {rank_list[i]}: {unique_devices[i]}'))
            i+=1
        return block_list
    
    ##############################################################################################################################
    
    def get_report_layout(self,fig_list:list,kpis_blocks:list,table:pd.DataFrame=None,device_id_main:str=None, report_type:str=None):
        
        header = dp.Group(dp.Media(self.properties.report_valoores_png_path), dp.HTML(self.properties.report_banner_html), columns=2, widths=[1, 5])
        
        devider = dp.HTML(self.properties.report_devider_html)
        
        test = dp.HTML(self.properties.test_html)
        
        device_main_kpi = dp.BigNumber(heading="Main Device ID",value=f"{device_id_main}",label="Device ID")

        dropdown_html = dp.HTML(self.properties.report_drowpdown_html) 
                           
        block_list = self.get_report_selector_blocks(fig_list,kpis_blocks,table,report_type)

        report = dp.Blocks(
        dp.Page(title="Data",blocks=[header,devider,test, device_main_kpi,dropdown_html,dp.Select( blocks=block_list ,type=dp.SelectType.DROPDOWN, label="Select Device")]
        ),
        dp.Page(title="Table", blocks=[dp.DataTable(table[['RANK','DEVICE_ID','GRID_SIMILARITY','COUNT','SEQUENCE_DISTANCE','LONGEST_SEQUENCE']], label="Data")]),
        )
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : Report Layout Done'))


        return report

    ##############################################################################################################################

    def get_geo_confidence_layout(self, device_confidence_report, table_id):

        # Initialize the block list
        block_list = []

        # Summary Statistics
        device_id = device_confidence_report['DEVICE_ID_GEO'].values[0]
        start_date = device_confidence_report['START_DATE'].values[0]
        end_date = device_confidence_report['END_DATE'].values[0]
        total_days = device_confidence_report['RECORDED_DAYS'].values[0]
        total_hits = device_confidence_report['RECORDED_HITS'].values[0]

        device_id_block = (dp.BigNumber(
                    heading="Device",
                    value=f"{device_id}",
                ))
        
        # Append the blocks and gauge_plot to a dp.Group
        kpi_group = dp.Group(
                            dp.BigNumber(
                                heading="Start Date",
                                value=f"{start_date}",
                            ),
                            dp.BigNumber(
                                heading="End Date",
                                value=f"{end_date}",
                            ),
                            dp.BigNumber(
                                heading="Total Hits",
                                value=f"{total_hits}",
                            ),
                            dp.BigNumber(
                                heading="Total Days",
                                value=f"{total_days}",
                            ),
                            columns=2
                        ) # Set widths for the components
        
        # Confidence Score Gauge Chart
        confidence_score_figure, confidence_score_description = self.geo_confidence_report.confidence_score_gauge_chart(device_confidence_report)
        block_list.extend([confidence_score_figure, confidence_score_description])
        
        gauge_plot = dp.Plot(confidence_score_figure['BLOCK_CONTENT'])
        confidence_score_explanation = dp.Text(confidence_score_description['BLOCK_CONTENT'])

        consistency_metrics_figure, consistency_metrics_description = self.geo_confidence_report.consistency_metrics_radar_chart(device_confidence_report)
        block_list.extend([consistency_metrics_figure, consistency_metrics_description])

        confidence_metrics_chart = dp.Plot(consistency_metrics_figure['BLOCK_CONTENT'])
        metrics_explanation = dp.Text(consistency_metrics_description['BLOCK_CONTENT'])
        
        confidence_metrics_group = dp.Group(
            dp.Group(gauge_plot, confidence_metrics_chart, columns = 2),
            dp.Group(confidence_score_explanation, metrics_explanation, columns = 2)
        )
        
        # Insert AOI - Geo Confidence Components in oracle
        self.insert_report_blocks_into_database(block_list, table_id)

        device_confidence_block = [
            device_id_block,
            kpi_group,
            confidence_metrics_group
        ]
            

        return device_confidence_block
        
    def get_report_layout_aoi(self, aoi_report : AOIReportFunctions, device_id, table_id):
        print(device_id)
        # Get AOI Location Addresses
        aoi_report.get_location_addresses()

        # Initialize the block list
        block_list = []

        # Get the number of hits per day of week, and the Days of Week with the highest and lowest hits
        hits_per_dow_figure, hits_per_dow_description = aoi_report.number_of_hits_per_dow()
        block_list.extend([hits_per_dow_figure, hits_per_dow_description])
        
        # Get the number of hits per month, and the Months with the highest and lowest hits
        hits_per_month_figure, hits_per_month_description = aoi_report.number_of_hits_per_month()
        block_list.extend([hits_per_month_figure, hits_per_month_description])

        # Get the location likelihood with respect to Day of Week
        location_likelihood_wrt_dow_figure, location_likelihood_wrt_dow_description = aoi_report.location_likelihood_per_dow()
        block_list.extend([location_likelihood_wrt_dow_figure, location_likelihood_wrt_dow_description])
        
        # Get the current location and estimated next location, if available, as well as the estimated duration at the current location, and the confidence score in the next location
        current_location, potential_next_location, location_confidence_score, expected_duration = aoi_report.next_likely_location()

        # Get the location duration with respect to the Day of Week
        location_duration_wrt_dow_figure, location_duration_wrt_dow_description = aoi_report.duration_at_aoi_wrt_dow()
        block_list.extend([location_duration_wrt_dow_figure, location_duration_wrt_dow_description])

        # Create the geospatial information report block
        geospatial_info_report = dp.Select(
            dp.Group(
                dp.Plot(hits_per_dow_figure['BLOCK_CONTENT'], label = "Number of Hits Per Day of Week"),
                dp.Text(hits_per_dow_description['BLOCK_CONTENT']),
                label = "Number of Hits Per Day of Week"
            ),
            dp.Group(
                dp.Plot(hits_per_month_figure['BLOCK_CONTENT'], label = "Number of Hits Per Month"), 
                dp.Text(hits_per_month_description['BLOCK_CONTENT']),
                label = "Number of Hits Per Month"
            )
        )

        # Get the current Weekday and Hour of Day from system time
        weekday_order = aoi_report.order[aoi_report.current_time.weekday()]
        current_hour = aoi_report.current_time.hour

        # Transform the duration to hour and minute format
        if isinstance(expected_duration, (int, float)):
            if expected_duration < 60:
                expected_duration = f"{expected_duration}M"
            else:
                hours = round(expected_duration // 60)
                remaining_minutes = round(expected_duration % 60)
                expected_duration = f"{hours}H-{remaining_minutes}M"

        # Create the Select component
        location_likelihood_block = dp.Select(
            dp.Group(
                dp.Plot(location_likelihood_wrt_dow_figure['BLOCK_CONTENT'], label = "Location Likelihood with respect to Day of Week"),
                dp.Group(location_likelihood_wrt_dow_description['BLOCK_CONTENT']),
                label = "Location Likelihood with respect to Day of Week"),
            dp.Group(
                dp.Plot(location_duration_wrt_dow_figure['BLOCK_CONTENT'], label = "Duration per Location with respect to Day of Week"),
                dp.Group(location_duration_wrt_dow_description['BLOCK_CONTENT']),
                label = "Duration per Location with respect to Day of Week")
            )

        if aoi_report.trajectory_displayed:
            
            print(f"The current location is: {current_location}")
            print(f"The potential next location is: {potential_next_location}")
            
            next_location_map = aoi_report.display_next_location_trajectory(current_location, potential_next_location)
            map_description = dp.Text(f"This map shows the predicted itinerary from the current location to the potential next location")
            itinerary_map = dp.Plot(next_location_map, label = "Predicted Itinerary")
            itinerary_group = dp.Group(itinerary_map, map_description, label = "Predicted Itinerary")
            
            next_location_map_antpath = aoi_report.display_next_location_trajectory_antpath(current_location, potential_next_location)
            map_description_antpath = dp.Text(f"This map shows the predicted itinerary from the current location to the potential next location")
            itinerary_map_antpath = dp.Plot(next_location_map_antpath, label = "Predicted Itinerary")
            itinerary_group_antpath = dp.Group(itinerary_map_antpath, map_description_antpath, label = "Predicted Itinerary Antpath")
            
            kpi_group = dp.Group(
                dp.Group(
                    dp.BigNumber(value = f"{weekday_order}", heading = "Current Day Of Week"),
                    dp.BigNumber(value = f"{current_hour}", heading = "Current Hour Of Day"),
                    columns = 2
                ),
                dp.BigNumber(value = f"{current_location}", heading = "Likely Current Location"),
                dp.Group(
                    dp.BigNumber(value = f"{expected_duration}", heading = "Expected Duration at Current Location"),
                    dp.BigNumber(value = f"{location_confidence_score}", heading = "Next Location Confidence Score"),
                    columns = 2
                ),
                dp.BigNumber(value = f"{potential_next_location}", heading = "Potential Next Location"),
                columns = 1,
                label= "Estimated Current Location and Predicted Next Location"
                )
            itinerary_block = dp.Select(
                kpi_group,
                itinerary_group,
                itinerary_group_antpath
            )
        else:
            kpi_group = dp.Group(
                dp.Group(
                    dp.BigNumber(value = f"{weekday_order}", heading = "Current Day Of Week"),
                    dp.BigNumber(value = f"{current_hour}", heading = "Current Hour Of Day"),
                    columns = 2
                ),
                dp.BigNumber(value = f"{current_location}", heading = "Likely Current Location"),
                columns = 1,
                label= "Estimated Current Location"
                )
            itinerary_block = dp.Group(dp.Text("### Estimated Current Location"), kpi_group)

        # Get the AOI Map
        aoi_map = aoi_report.aoi_display()
        aoi_map = dp.Plot(aoi_map, label="Device AOIs")
        
        # Get the AOI dataframe
        aoi_data = aoi_report.aoi_dataframe()
        
        # Get the Device Suspciciousness Results
        suspicion_results = aoi_report.suspiciousness_result()

        # If device is suspicious, display the suspiciousness results in a table. Else, display a message.
        if not suspicion_results.empty:
            suspicion_results_block = dp.DataTable(suspicion_results, label="Suspiciousness Evaluation")
        else:
            suspicion_results_block = dp.Text("Suspiciousness Evaluation is Currently Unavailable", label="Suspiciousness Evaluation")

        # Create the AOI Summary Statistics block
        aoi_summary_results = dp.Select(
            blocks=[
                dp.DataTable(aoi_data, label= "AOIs Summary Statistics"),
                suspicion_results_block,
            ]
        )

        # Save AOI Components in database
        self.insert_report_blocks_into_database(block_list, table_id)

        # Create the final report
        final_aoi_report = [
            geospatial_info_report,
            location_likelihood_block,
            itinerary_block,
            aoi_map,
            aoi_summary_results
        ]

        # Create Plot Dictionary
        report_items_dictionary = {
            'AOI Table': [aoi_data],
            'AOI Map': [aoi_map],
            'Suspiciousness Results': [suspicion_results],
            'Location Likelihood wrt Dow': [location_likelihood_wrt_dow_figure['BLOCK_CONTENT'], location_likelihood_wrt_dow_description['BLOCK_CONTENT']],
            'Location Duration wrt Dow': [location_duration_wrt_dow_figure['BLOCK_CONTENT'], location_duration_wrt_dow_description['BLOCK_CONTENT']],
        }

        return final_aoi_report, report_items_dictionary

    def merge_reports_to_select_dropdown(self, blocks_aoi):
        group_list = []

        for device_dict in blocks_aoi:
            device_id = list(device_dict.keys())[0]
            blocks_obj = list(device_dict.values())[0]

            # group = dp.Group(blocks=[blocks_obj], label=device_id)
            group_list.append(blocks_obj)
            # group = dp.Group(blocks=[blocks_obj], label=device_id)
            group_list.append(blocks_obj)

        # Initialize the report with a Select block containing the groups
        final_report = dp.Report(
            dp.Group(dp.Media(self.properties.report_valoores_png_path), dp.HTML(self.properties.report_banner_html), columns=2, widths=[1, 5]),
            dp.HTML(self.properties.report_devider_html),
            dp.Select(blocks=group_list, type=dp.SelectType.DROPDOWN)
        )

        return final_report
    
    def get_information_page(self):
        info_page_tables = []
        if self.geo_confidence_flag:
            data = {
                'Metrics' : ['Time Period Consistency', 'Hour of Day Consistency', 'Day of Week Consistency', 'Hits Per Day Consistency', 'Confidence Score'],
                'Description': [
                    "This metric evaluates the consistency of days recorded over the specified period of time.",
                    "This metric assesses the consistency of data recorded throughout the day.",
                    "This metric assesses the consistency of recorded data with respect to the Day of Week.",
                    "This metric evaluates the consistency of activity levels recorded per day, providing insights into user behavior patterns throughout a day.",
                    """This score provides an overall measure of confidence in the analysis results, based on the availability and consistency of geospatial data. 
                    This metrics, based on the aforementioned consistency metrics, ranges between 0 and 100, 
                    and indicates that the data and analysis results are reliable the higher this score is."""
                ]
            }

            df = pd.DataFrame(data)
            df.set_index('Metrics', inplace=True)
            # styled_df = df.style.set_properties(subset=['Metrics'], **{'width': '200px'})
            styled_df = df.style.set_properties(**{'text-align': 'left'})
            geo_confidence_info_page = dp.Table(styled_df, label = "Geospatial Confidence Metrics")
            table_title = dp.Text("## Geospatial Confidence Metrics")
            info_page_tables.append(table_title)
            info_page_tables.append(geo_confidence_info_page)

        if self.aoi_flag:
            data = {
                'Key Words' : [
                    'Device Drop Down Menu', 'Start and End Dates', 'Total Hits', 'Total Days', 'Geospation Confidence Metrics',
                    'Number of Hits Per Day of the Week', 'Number of Hits Per Month', 'Location Likelihood with respect to Day of Week', 'Estimated Current Location',
                    'Duration per Location with respect to Day of Week', 'Upcoming', 'Map', 'AOIs Summary Statistics', 'Suspiciousness Evaluation'
                ],
                'Description' : [
                    'This is a dropdown menu that allows the user to select a device from the list of devices.',
                    'Shows the start and end dates of the selected period of time.',
                    'Shows the total number of hits recorded in the selected period of time.',
                    'Shows the total number of days recorded in the selected period of time.',
                    'Shows the geospatial confidence metrics for the selected device.',
                    'Shows the number of hits per day of the week for the selected device.',
                    'Shows the number of hits per month for the selected device.',
                    'Shows the location likelihood with respect to day of week for the selected device.',
                    """Shows the estimated current location for the selected device based on the device history and the current time. 
                    This also shows the expected duration of stay at the current location along with the potential next location and the latter's confidence score.""",
                    'Shows the duration per location with respect to day of week for the selected device.',
                    'upcoming',
                    'An interactive map that shows the location of the selected device along with its coordinates, type, and number of hits.',
                    'Shows the different AOIs for the selected device along with their coordinates, type, number of hits, and total days recorded.',
                    'Shows the suspiciousness evaluation for the selected device. This evaluation is based on how likely this device is to be in location or not based on its history.'
                ]
            }

            df = pd.DataFrame(data)
            df.set_index('Key Words', inplace=True)
            # styled_df = df.style.set_properties(subset=['Key Words'], **{'width': '200px'})
            styled_df = df.style.set_properties(**{'text-align': 'left'})
            aoi_info_page = dp.Table(styled_df, label = "AOI Key Words Description")
            table_title = dp.Text("## AOI Key Words Description")
            info_page_tables.append(table_title)
            info_page_tables.append(aoi_info_page)

        if self.cdr_flag:
            data = {
                'Key Words' : [],
                'Description' : []
            }

            df = pd.DataFrame(data)
            df.set_index('Key Words', inplace=True)
            # styled_df = df.style.set_properties(subset=['Key Words'], **{'width': '200px'})
            styled_df = df.style.set_properties(**{'text-align': 'left'})
            cdr_info_page = dp.Table(styled_df, label = "CDR Key Words Description")
            table_title = dp.Text("## CDR Key Words Description")
            info_page_tables.append(table_title)
            info_page_tables.append(cdr_info_page)

        if self.cotraveler_flag:
            data = {
                'Key Words' : [],
                'Description' : []
            }

            df = pd.DataFrame(data)
            df.set_index('Key Words', inplace=True)
            # styled_df = df.style.set_properties(subset=['Key Words'], **{'width': '200px'})
            styled_df = df.style.set_properties(**{'text-align': 'left'})
            cotraveler_info_page = dp.Table(styled_df, label = "Cotraveler Key Words Description")
            table_title = dp.Text("## Cotraveler Key Words Description")
            info_page_tables.append(table_title)
            info_page_tables.append(cotraveler_info_page)

        blocks = [*info_page_tables]
        # info_page_grouped = dp.Group(blocks = blocks)
        info_page_report = dp.Report(*blocks)
        return info_page_report

    ##############################################################################################################################
    
    def get_report_layout_cdr(self,blocks,table, report_type:str=None):
        header = dp.Group(dp.Media(self.properties.report_valoores_png_path), dp.HTML(self.properties.report_banner_html), columns=2, widths=[1, 5])
        
        devider = dp.HTML(self.properties.report_devider_html)
        print(table.columns)
        device_main_kpi = dp.Group(dp.BigNumber(heading="IMSI",value=f"{table['imsi_id'][0]}",label="IMSI"),
                                   dp.BigNumber(heading="Phone Number",value=f"{table['phone_number'][0]}",label="Phone Number"),
                                   dp.BigNumber(heading="IMEI",value=f"{table['imei_id'][0]}",label="IMEI"),
                                   dp.BigNumber(heading="Service Provider ID",value=f"{table['service_provider_id'][0]}",label="Service Provider ID"),
                                   columns=2)

        blocks = self.get_report_blocks_cdr(blocks)

        report = dp.Blocks(
        dp.Page(title="Data",blocks=[header,devider,device_main_kpi,dp.Group(blocks = blocks)]
        ),
        dp.Page(title="Table", blocks=[dp.DataTable(table, label="Data")]),
        )
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★'.format('Reporting : Report Layout Done'))


        return report

    ##############################################################################################################################
    def get_simulation_report_layout(self,blocks,main_table , report_table , report_type:int=None,report_name:str=None,description:str=None,table_id:int=None,blocks_aoi_dictionary:dict={},blocks_cotravelers_dictionary:dict={}):
        
        report_saved_name = main_table.loc[0, 'LOC_REPORT_NAME']
        report_saved_name = 'Not Available' if report_name == 'No Name' else report_saved_name
        # Initialize block list
        block_list = []
        if report_type == 1:
            title = 'Activity Scan'
        if report_type == 2:
            title = 'Device History'
        if report_type == 6:
            title = 'Device Travel Pattern'
        # else:
        #     title = 'TCD History'

        with open(self.properties.report_valoores_png_path, 'rb') as file:
            binary_data = file.read()

            # Encode the binary data to Base64
        base64_encoded = base64.b64encode(binary_data).decode('utf-8')
        header = dp.HTML(self.properties.report_banner_html.format(title, report_saved_name,base64_encoded))

        # devider = dp.HTML(self.properties.report_devider_html)
        # report_introduction =  dp.HTML(html=self.properties.device_history_template,name=f"{report_saved_name}")
        try:
            start_date = main_table.loc[0, 'STATUS_BDATE'].date() if main_table.loc[0, 'STATUS_BDATE'].date() is not None else 'Not Available'
        except:
            start_date = 'Not Available' 
        try:
            end_date = main_table.loc[0, 'FILTER_EDATE'].date()
        except:
            end_date = 'Not Available'


        # Access the first elements from the lists
        # Accessing the first element of the lists in the first row
        # country_name = blocks[4]['Country list'][0]
        # city_name = blocks[4]['City list'][0]
        country_name = coco.convert(names=blocks['summary_statistics']['Country list'][0], to='name_short')
        # country_name = ', '.join(country_name).strip('[]')
        
        city_name = ', '.join(blocks['summary_statistics']['City list'][0]).strip('[]')

        device_ids = report_table.loc[:, 'DEVICE_ID'].unique()


        total_number_of_hits = len(report_table)

        if len(device_ids) == 1:
            device_or_number = device_ids[0]
        else:
            device_count = len(device_ids)
            device_or_number = device_count
        print('START DATE', start_date)
        print('END DATE', end_date)
        if report_type == 2:
            report_introduction = self.properties.device_history_template.format(report_saved_name=report_saved_name, start_date=start_date, end_date=end_date,device_or_number=device_or_number,total_number_of_hits=total_number_of_hits,country_name=country_name,city_name=city_name)
        if report_type == 1:
            report_introduction =  self.properties.activity_scan_template.format(report_saved_name=report_saved_name, start_date=start_date, end_date=end_date,device_or_number=device_or_number,total_number_of_hits=total_number_of_hits,country_name=country_name,city_name=city_name)
        if report_type==6:
            report_introduction =  self.properties.DHP_template.format(report_saved_name=report_saved_name, start_date=start_date, end_date=end_date,device_or_number=device_or_number,total_number_of_hits=total_number_of_hits,country_name=country_name,city_name=city_name)
        
        # user_prompt = f"""

        #     user input : {description}

        #     description :{report_introduction}

        #     """

        # system_prompt = """
        #     [INST] <<SYS>>
        #     You are tasked with crafting an engaging introduction that encapsulates the essence of the user's description and the information provided in the system prompt. Your role is to:

        #     - Incorporate the key details from the user's description and the user input, ensuring all relevant information is included.
        #     - The user prompt consists of user input followed by the description.
        #     - Incorporate all details from the user's input and description 
        #     - Maintain a formal and professional tone, suitable for a narrative that could be shared in a legal or investigative context.
        #     - Write it in a html formatte for example add <br> when you need to go to another line and </b> where the letter should be in bold as the discription.
        #     <</SYS>>
        #     """
        # system_prompt = """
        #     [INST] <<SYS>>
        #     You are tasked with crafting an engaging introduction that encapsulates the essence of the user's description and the information provided in the system prompt. Your role is to:

        #     - Be engaging and compelling, capturing the reader's interest from the first sentence.
        #     - Incorporate the key details from the user's description and the system prompt, ensuring all relevant information is included.
        #     - The user prompt consists of user input followed by the description.
        #     - The introduction should end by the description of the user prompt.
        #     - Incorporate all details from the user's input and description 
        #     - Maintain a formal and professional tone, suitable for a narrative that could be shared in a legal or investigative context.
        #     - If the user's input is not provided, retrieve the description from the user prompt.
        #     - Write it in a html formatte for example add <br> when you need to go to another line.
        #     <</SYS>>
        #     """
        # Construct the full prompt including the system prompt and user input
        # full_prompt = f"{system_prompt}{user_prompt}[/INST]"
        # print(full_prompt)
        # # Initialize the Ollama model
        # llm = Ollama(model="llama2:latest")
        # llm.temperature = 0

        # # Invoke the model with the full prompt
        # report_introduction_ai = llm.invoke(full_prompt)
        introduction_block = {
            'BLOCK_CONTENT': report_introduction, 
            'BLOCK_NAME': 'Report Introduction', 
            'BLOCK_TYPE': 'HTML',
            'BLOCK_ASSOCIATION': 'Simulation',
            'DEVICE_ID': 'id'
        }
        block_list.extend([introduction_block])

        self.insert_report_blocks_into_database(block_list = block_list, table_id = table_id)

        report_introduction_html = dp.HTML(self.format_text('Summary',report_introduction))
        # print(self.properties.DHP_template.format(report_saved_name=report_saved_name, start_date=start_date, end_date=end_date,device_or_number=device_or_number))
        blocks,main_map = self.get_report_blocks_simulation(blocks,report_type)
        if blocks_aoi_dictionary!={}:
            print('AOI Condistion')
            aoi_blocks = self.get_report_blocks_aoi(blocks_aoi_dictionary)
            blocks.append(aoi_blocks)
            report = dp.Blocks(title="Data",blocks=[header,report_introduction_html,main_map, dp.Select(blocks = blocks)])
        if blocks_cotravelers_dictionary!={}:
            print('Cotraveler Condistion')
            cotraveler_blocks = self.get_report_blocks_cotravelers(blocks_cotravelers_dictionary)
            blocks.append(cotraveler_blocks)
            report = dp.Blocks(title="Data",blocks=[header,report_introduction_html,main_map, dp.Select(blocks = blocks)])
        else:
            report = dp.Blocks(title="Data",blocks=[header,report_introduction_html,main_map, dp.Select(blocks=blocks)])
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★'.format('Reporting : Report Layout Done'))

        return report,report_introduction_html
    
    ##############################################################################################################################

    def save_report(self,report,report_path:str = None,report_name:str = None ,open:bool=False):
            report_full_path = f"{report_path}/{report_name}.html"

            if isinstance(report, dp.Report):
                report.save(path = report_full_path)
            else:
                dp.save_report(path = report_full_path, blocks = report, open=open)
                print("saved in : ",report_full_path)

    ##############################################################################################################################

    def get_head_scripts(self, html_file_path):
        print("html_file_pathhtml_file_pathhtml_file_path", html_file_path)
        # Read the HTML content from the file
        with open(html_file_path, 'r', encoding='utf-8') as file:
            html_content = file.read()

        # Parse the HTML content
        soup = BeautifulSoup(html_content, 'lxml')

        # Find the <head> section
        head = soup.head

        # Extract all <script> elements within the <head>
        scripts = head.find_all('script')

        pretty_scripts = [str(script.prettify()) for script in scripts]
        # Return the script tags
        combined_pretty_scripts = '\n'.join(pretty_scripts)
        
        return combined_pretty_scripts
        
    ##############################################################################################################################
    def get_scripts(self, html_file_path):
        print("html_file_pathhtml_file_pathhtml_file_path", html_file_path)
        # Read the HTML content from the file
        with open(html_file_path, 'r', encoding='utf-8') as file:
            html_content = file.read()

        # Parse the HTML content
        soup = BeautifulSoup(html_content, 'lxml')
        soup = str(soup.prettify())
        
        return soup
    def insert_into_database(self, file_name, script_contents, table_id):
        cursor, connection = self.oracle_tools.get_oracle_connection()
        print(f'ssdx_tmp.tmp_Analytics_Report_{table_id}')
        # Insert the body content and file name into the PL/SQL table
        cursor.execute(f"INSERT INTO ssdx_tmp.tmp_Analytics_Report_{table_id} (TYPE_PLOT, HTML_FILE) VALUES (:file_name, :body)",
                    {'file_name': file_name, 'body': script_contents})

        # Commit changes
        connection.commit()
        
        print(f"Inserted {file_name} into the database.")
        
    ##############################################################################################################################
        
    def get_html_files(self,file_name, html_file_path:str ,table_id:str):  
        body_content = self.get_head_scripts(html_file_path)
        # body_content=json.dump(body_content)
        self.insert_into_database(file_name,body_content,table_id)

        print(f"Invalid input: {html_file_path} is not a valid directory or HTML file.")
            
    ##############################################################################################################################

    def insert_report_block_into_database(self, block, table_id, cursor, connection):

        if block['BLOCK_TYPE'] == 'PLOTLY':
            block['BLOCK_CONTENT'] = block['BLOCK_CONTENT'].to_json()

        if block['BLOCK_TYPE'] == 'table':
            block['BLOCK_CONTENT'] = block['BLOCK_CONTENT'].to_json(orient="records")

        if block['BLOCK_TYPE'] == 'folium':
            block['BLOCK_CONTENT'] = block['BLOCK_CONTENT'].get_root().render()
        if block['BLOCK_TYPE'] == 'LIST':
            block['BLOCK_CONTENT'] = json.dumps(block['BLOCK_CONTENT'])

        # Insert the body content and file name into the PL/SQL table
        cursor.execute(f"INSERT INTO SSDX_TMP.Analytics_Simulation_Blocks_{table_id} (BLOCK_NAME, BLOCK_CONTENT, BLOCK_TYPE, BLOCK_ASSOCIATION, DEVICE_ID) VALUES (:block_name, :block_content, :block_type,:block_association, :device_id)",
                    {'block_name': block['BLOCK_NAME'], 'block_content': block['BLOCK_CONTENT'], 'block_type': block['BLOCK_TYPE'], 'block_association': block['BLOCK_ASSOCIATION'], 'device_id': block['DEVICE_ID']})
        
        connection.commit()

        print(f"Inserted {block['BLOCK_NAME']} into the database --", block['BLOCK_ASSOCIATION'])

    def insert_report_blocks_into_database(self, block_list, table_id):
        cursor, connection = self.oracle_tools.get_oracle_connection()
        print(f'SSDX_TMP.Analytics_Simulation_Blocks_{table_id}')
        try:
            self.oracle_tools.drop_create_table(table_name = self.properties.oracle_analytcis_simulation_blocks_table, table_id = table_id , table_schema_query = self.properties._oracle_table_schema_query_analytics_report_blocks,drop=False)
        except:
            print("Table already exist")
        for block in block_list:
            self.insert_report_block_into_database(block, table_id, cursor, connection)       
        
        # Commit changes
        connection.commit()
        connection.close()

    def get_report_blocks_table(self,table_id):
        result = {}
        cursor, connection = self.oracle_tools.get_oracle_connection(clob=True)
        cursor.execute(f"SELECT block_name,block_type,block_association,device_id, SUBSTR(block_content,1,10000000) as block_content FROM ssdx_tmp.Analytics_Simulation_Blocks_{table_id}")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        result = pd.DataFrame(rows, columns=columns)
        connection.commit()
        connection.close()
        return result