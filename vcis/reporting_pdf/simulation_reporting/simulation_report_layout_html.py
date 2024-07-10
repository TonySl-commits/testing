import datapane as dp
import re
from pretty_html_table import build_table
import base64

from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.reporting.geo_reporting.geo_confidence_report.geo_confidence_report_functions import GeoConfidenceReportFunctions
from vcis.reporting.geo_reporting.aoi_report.aoi_report_functions import AOIReportFunctions
# dp.enable_logging()
class SimulationReportLayoutHTML:
    def __init__(self, verbose=False):
        self.properties = CDR_Properties()
        self.utils = CDR_Utils(verbose=True)
        self.cassandra_tools = CassandraTools()
        self.cassandra_spark_tools = CassandraSparkTools()
        self.oracle_tools = OracleTools() 
    def format_text(self,title:str=None, text:str=None , header:int=2):
        # Define the CSS styles
        css_style = """
        <style>
        .custom-text {
                color: #002147;
                font-family: 'Times New Roman', Times, serif;
                text-align: justify;
            }
        </style>
        """

        # Create the HTML structure
        html_content = f"""
        {css_style}
        <div class="custom-text">
            <h{header}>{title}</h{header}>
            <p>{text}</p>
        </div>
        """

        return html_content
    
    def format_title(self,title:str=None, header:int=1):
        # Define the CSS styles
        css_style = """
        <style>
        .custom-text {
                color: #002147;
                font-family: 'Times New Roman', Times, serif;
                text-align: justify;
            }
        </style>
        """

        # Create the HTML structure
        html_content = f"""
        {css_style}
        <div class="custom-text">
            <h{header}>{title}</h{header}>
        </div>
        """

        return html_content
    # def format_text(self,title:str=None, text:str=None,title_bool:bool=True,text_bool=True):
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
    #     if title_bool==True and text_bool==True:
    #         # Create the HTML structure
    #         html_content = f"""
    #         {css_style}
    #         <div class="custom-text">
    #             <h1>{title}</h1>
    #             <p>{text}</p>
    #         </div>
    #         """

    #     if title_bool==True and text_bool==False:
    #         html_content = f"""
    #         {css_style}
    #         <div class="custom-text">
    #             <h1>{title}</h1>
    #         </div>
    #         """
    #     if title_bool==False and text_bool==True:
    #         html_content = f"""
    #         {css_style}
    #         <div class="custom-text">
    #             <p>{text}</p>
    #         </div>
    #         """
    #     else:
    #         html_content=""

    #     return html_content
 

        
        return html_content
    def get_simulation_report_layout_ac(self,title,
                                        table_id ,
                                        abstract ,
                                        introduction ,
                                        summary_table ,
                                        dow_fig,
                                        activity_scan_hits_distribution ,
                                        timespent_analysis ,
                                        timespent_insights_observation ,
                                        hourly_activity,
                                        most_active_devices,
                                        device_activity_patterns,
                                        hourly_activty_fig,
                                        hourly_activity_df,
                                        conclusion ,
                                        mapping_table):
                

        header_valoores = dp.Group(dp.Media(self.properties.report_valoores_png_path), dp.HTML(self.properties.report_banner_html.format(title)), columns=2, widths=[1, 12])
        
        devider = dp.HTML(self.properties.report_devider_html)


        abstract = dp.HTML(self.format_text('Abstract',abstract))

        summary_table = summary_table.style.background_gradient(cmap='PuBu')

        summary_table = dp.Table(summary_table)

        introduction = dp.HTML(self.format_text('Introduction',introduction))

        activity_scan_hits_distribution_html = ""
        header_pattern = r'\*\*(.*?)\*\*'
        headers = re.findall(header_pattern, activity_scan_hits_distribution)
        for header in headers:
            placeholder = f'<b>{header}</b>'
            activity_scan_hits_distribution = activity_scan_hits_distribution.replace(f'**{header}**', placeholder)
        
         # Split the reports by new lines and iterate over them
        for report in activity_scan_hits_distribution.split('\n\n'):
            # Skip empty lines
            if not report.strip():
                continue
            
                # Split each report into report number and content
            report_lines = report.split('\n')
            
            report_paragraphs = f"• {report}<br>"
            activity_scan_hits_distribution_html+= report_paragraphs
            
        activity_scan_hits_distribution = dp.HTML(self.format_text('Analysis of The Acivity Scan Hits Distribution DOW',activity_scan_hits_distribution_html))

        dow_fig = dp.Plot(dow_fig)

        timespent_analysis_html = ""
        header_pattern = r'\*\*(.*?)\*\*'
        headers = re.findall(header_pattern, timespent_analysis)
        for header in headers:
            placeholder = f'<b>{header}</b>'
            timespent_analysis = timespent_analysis.replace(f'**{header}**', placeholder)
        
         # Split the reports by new lines and iterate over them
        for report in timespent_analysis.split('\n\n'):
            # Skip empty lines
            if not report.strip():
                continue
            
                # Split each report into report number and content
            report_lines = report.split('\n')
            
            report_paragraphs = f"• {report}<br>"
            timespent_analysis_html+= report_paragraphs

        
            
        activity_scan_hits_distribution = dp.HTML(self.format_text('Analysis of The Acivity Scan Hits Distribution DOW',activity_scan_hits_distribution_html),label='Analysis of The Acivity Scan Hits Distribution DOW')


        timespent_analysis = dp.HTML(self.format_text('TimeSpent Analysis at AOI',timespent_analysis_html),label='TimeSpent Analysis at AOI')

        timespent_insights_observation =  dp.HTML(self.format_text('Insights and Observations',timespent_insights_observation),label='Insights and Observations')

        hourly_activity = dp.HTML(self.format_text('Hourly Device Activity at AOI',hourly_activity))
        hourly_activty_fig  = dp.Plot(hourly_activty_fig)
        hourly_activity_df = dp.DataTable(hourly_activity_df)
        hourly_activity_display = dp.Group(blocks = [hourly_activty_fig,hourly_activity_df],columns=2)
        most_active_devices_html = ""
        for report in most_active_devices.split('*'):
            # Skip empty lines
            if not report.strip():
                continue
            
                # Split each report into report number and content
            report_lines = report.split('\n')
            
            report_paragraphs = f"• {report}<br>"
            most_active_devices_html+= report_paragraphs



        most_active_devices = dp.HTML(self.format_text('Most Active Devices by Hour',most_active_devices_html),label='Most Active Devices by Hour')

        device_activity_patterns_html = ""
        for report in device_activity_patterns.split('*'):
            # Skip empty lines
            if not report.strip():
                continue
            
                # Split each report into report number and content
            report_lines = report.split('\n')
            
            report_paragraphs = f"• {report}<br>"
            device_activity_patterns_html+= report_paragraphs


        device_activity_patterns= dp.HTML(self.format_text('Device Activity Patterns',device_activity_patterns_html),label='Device Activity Patterns')

        conclusion =  dp.HTML(self.format_text('Conclusion',conclusion))

        dow_analysis = dp.Select(blocks=[dp.Group(blocks=[activity_scan_hits_distribution,dow_fig],label='Analysis of The Acivity Scan Hits Distribution DOW'),timespent_analysis,timespent_insights_observation])
        hourly_analysis =   dp.Select(blocks=[dp.Group(blocks=[hourly_activity, hourly_activity_display] , label='Hourly Device Activity at AOI') , most_active_devices, device_activity_patterns]) 
        report = dp.Blocks(
        # dp.Page(title="Report",blocks=[ header_valoores, abstract, summary_table, introduction, activity_scan_hits_distribution ,dow_fig, timespent_analysis, timespent_insights_observation, hourly_activity, hourly_activity_display , most_active_devices, device_activity_patterns, conclusion]
        dp.Page(title="Report",blocks=[ header_valoores, abstract, summary_table, introduction,dow_analysis, hourly_analysis, conclusion]

        ),
        dp.Page(title="Devices Mapping", blocks=[dp.DataTable(mapping_table)]),
        )

        return report
    # def get_simulation_report_layout_ac(self,title,
    #                                     table_id ,
    #                                     abstract ,
    #                                     introduction ,
    #                                     summary_table ,
    #                                     dow_fig,
    #                                     activity_scan_hits_distribution ,
    #                                     timespent_analysis ,
    #                                     timespent_insights_observation ,
    #                                     hourly_activity,
    #                                     most_active_devices,
    #                                     device_activity_patterns,
    #                                     hourly_activty_fig,
    #                                     hourly_activity_df,
    #                                     conclusion ,
    #                                     mapping_table):
                

    #     header_valoores = dp.Group(dp.Media(self.properties.report_valoores_png_path), dp.HTML(self.properties.report_banner_html.format(title)), columns=2, widths=[1, 12])
        
    #     devider = dp.HTML(self.properties.report_devider_html)


    #     abstract = dp.HTML(self.format_text('Key Takeaways and Crucial Findings',abstract))

    #     summary_table = summary_table.style.background_gradient(cmap='PuBu')

    #     summary_table = dp.Table(summary_table)

    #     introduction = dp.HTML(self.format_text('Introduction',introduction))

    #     activity_scan_hits_distribution_html = ""
    #     header_pattern = r'\*\*(.*?)\*\*'
    #     headers = re.findall(header_pattern, activity_scan_hits_distribution)
    #     for header in headers:
    #         placeholder = f'<b>{header}</b>'
    #         activity_scan_hits_distribution = activity_scan_hits_distribution.replace(f'**{header}**', placeholder)
        
    #      # Split the reports by new lines and iterate over them
    #     for report in activity_scan_hits_distribution.split('\n\n'):
    #         # Skip empty lines
    #         if not report.strip():
    #             continue
            
    #             # Split each report into report number and content
    #         report_lines = report.split('\n')
            
    #         report_paragraphs = f"• {report}<br>"
    #         activity_scan_hits_distribution_html+= report_paragraphs
            
    #     activity_scan_hits_distribution = dp.HTML(self.format_text('Analysis of The Acivity Scan Hits Distribution DOW',activity_scan_hits_distribution_html))

    #     dow_fig = dp.Plot(dow_fig)

    #     timespent_analysis_html = ""
    #     header_pattern = r'\*\*(.*?)\*\*'
    #     headers = re.findall(header_pattern, timespent_analysis)
    #     for header in headers:
    #         placeholder = f'<b>{header}</b>'
    #         timespent_analysis = timespent_analysis.replace(f'**{header}**', placeholder)
        
    #      # Split the reports by new lines and iterate over them
    #     for report in timespent_analysis.split('\n\n'):
    #         # Skip empty lines
    #         if not report.strip():
    #             continue
            
    #             # Split each report into report number and content
    #         report_lines = report.split('\n')
            
    #         report_paragraphs = f"• {report}<br>"
    #         timespent_analysis_html+= report_paragraphs

        
            
    #     activity_scan_hits_distribution = dp.HTML(self.format_text('Analysis of The Acivity Scan Hits Distribution DOW',activity_scan_hits_distribution_html),label='Analysis of The Acivity Scan Hits Distribution DOW')


    #     timespent_analysis = dp.HTML(self.format_text('TimeSpent Analysis at AOI',timespent_analysis_html),label='TimeSpent Analysis at AOI')

    #     timespent_insights_observation =  dp.HTML(self.format_text('Insights and Observations',timespent_insights_observation),label='Insights and Observations')
    #     terminals = ['QT-QATAR-Terminal2', 'QT_QATAR-Terminal2-EMIRI', 'QT_QATAR-Terminal1']
    #     i=0
    #     hourly_activity = dp.HTML(self.format_text('Hourly Device Activity at AOI',hourly_activity))

    #     hourly_activity_display = dp.Select(blocks=[dp.Group(blocks=[dp.Plot(hourly_activty_fig[0]),dp.DataTable(hourly_activity_df[0])],label='QT_QATAR-Terminal1',columns=2),dp.Group(blocks=[dp.Plot(hourly_activty_fig[1]),dp.DataTable(hourly_activity_df[1])],label='QT-QATAR-Terminal2',columns=2),dp.Group(blocks=[dp.Plot(hourly_activty_fig[2]),dp.DataTable(hourly_activity_df[2])],label='QT_QATAR-Terminal2-EMIRI',columns=2)])

    #     # hourly_activity_display = dp.Group(blocks = [hourly_activty_fig,hourly_activity_df],columns=2)
    #     most_active_devices_html = ""
    #     for report in most_active_devices.split('*'):
    #         # Skip empty lines
    #         if not report.strip():
    #             continue
            
    #             # Split each report into report number and content
    #         report_lines = report.split('\n')
            
    #         report_paragraphs = f"• {report}<br>"
    #         most_active_devices_html+= report_paragraphs



    #     most_active_devices = dp.HTML(self.format_text('Most Active Devices by Hour',most_active_devices_html),label='Most Active Devices by Hour')

    #     device_activity_patterns_html = ""
    #     for report in device_activity_patterns.split('*'):
    #         # Skip empty lines
    #         if not report.strip():
    #             continue
            
    #             # Split each report into report number and content
    #         report_lines = report.split('\n')
            
    #         report_paragraphs = f"• {report}<br>"
    #         device_activity_patterns_html+= report_paragraphs


    #     device_activity_patterns= dp.HTML(self.format_text('Device Activity Patterns',device_activity_patterns_html),label='Device Activity Patterns')

    #     conclusion =  dp.HTML(self.format_text('Conclusion',conclusion))

    #     dow_analysis = dp.Select(blocks=[dp.Group(blocks=[activity_scan_hits_distribution,dow_fig],label='Analysis of The Acivity Scan Hits Distribution DOW'),timespent_analysis,timespent_insights_observation])
    #     hourly_analysis =   dp.Select(blocks=[dp.Group(blocks=[hourly_activity, hourly_activity_display] , label='Hourly Device Activity at AOI') , most_active_devices, device_activity_patterns]) 
    #     report = dp.Blocks(
    #     # dp.Page(title="Report",blocks=[ header_valoores, abstract, summary_table, introduction, activity_scan_hits_distribution ,dow_fig, timespent_analysis, timespent_insights_observation, hourly_activity, hourly_activity_display , most_active_devices, device_activity_patterns, conclusion]
    #     dp.Page(title="Report",blocks=[ header_valoores, abstract, summary_table, introduction,dow_analysis, hourly_analysis, conclusion]

    #     ),
    #     dp.Page(title="Devices Mapping", blocks=[dp.DataTable(mapping_table)]),
    #     )

    #     return report
                
    # def get_simulation_report_layout_dh(self,dh_dictionary:dict=None):

    #     title = dh_dictionary['title']
    #     table_id = dh_dictionary['table_id']
    #     main_map = dh_dictionary['main_map']
    #     abstract = dh_dictionary['abstract']
    #     introduction = dh_dictionary['introduction']
    #     summary_table = dh_dictionary['summary_table']
    #     movement_analysis = dh_dictionary['movement_analysis']
    #     threat_analysis = dh_dictionary['threat_analysis']
    #     threat_score_table = dh_dictionary['threat_score_table']
    #     fig_devices_movments = dh_dictionary['fig_devices_movments']
    #     patterns_links = dh_dictionary['patterns_links']
    #     cotraveler_table = dh_dictionary['cotraveler_table']
    #     location_likelihood_Observation = dh_dictionary['location_likelihood_Observation']
    #     location_likelihood_analysis = dh_dictionary['location_likelihood_analysis']
    #     cotraveler_barchart_dow_desctription = dh_dictionary['cotraveler_barchart_dow_desctription']
    #     cotraveler_barchart_analysis = dh_dictionary['cotraveler_barchart_analysis']
    #     cotraveler_barchart = dh_dictionary['cotraveler_barchart']
    #     heatmap_plots = dh_dictionary['heatmap_plots']
    #     aoi_table = dh_dictionary['aoi_table']
    #     aoi_mapp = dh_dictionary['aoi_mapp']
    #     # suspiciousness_results = dh_dictionary['suspiciousness_results']
    #     location_likelihood_wrt_dow_fig = dh_dictionary['location_likelihood_wrt_dow_fig']
    #     location_duration_wrt_dow_fig = dh_dictionary['location_duration_wrt_dow_fig']
    #     # location_duration_wrt_dow_description = dh_dictionary['location_duration_wrt_dow_description']
    #     location_likelihood_duration_Observation = dh_dictionary['location_likelihood_duration_Observation']
    #     location_likelihood_duration_analysis = dh_dictionary['location_likelihood_duration_analysis']
    #     conclusion = dh_dictionary['conclusion']
    #     mapping_table = dh_dictionary['mapping_table']
        
    #     header_valoores = dp.Group(dp.Media(self.properties.report_valoores_png_path), dp.HTML(self.properties.report_banner_html.format(title)), columns=2, widths=[1, 12])
        
    #     devider = dp.HTML(self.properties.report_devider_html)


    #     abstract = dp.HTML(self.format_text('Key Takeaways and Crucial Findings',abstract))
    #     html_table = summary_table.to_html(classes='table-style', escape=False,index=False)

    #     css_set_column_width = self.properties.css_set_column_width

    #     summary_table = css_set_column_width + html_table
    #     html_table = threat_score_table.to_html(classes='table-style', escape=False,index=False)
    #     threat_score_table = css_set_column_width + html_table
    #     html_table = cotraveler_table.to_html(classes='table-style', escape=False,index=False)
    #     cotraveler_table = css_set_column_width + html_table
    #     html_table = aoi_table.to_html(classes='table-style', escape=False,index=False)
    #     aoi_table = css_set_column_width + html_table
    #     # summary_table = dp.Table(summary_table)
    #     summary_table = dp.HTML(summary_table)
    #     introduction = dp.HTML(self.format_text('Introduction',introduction))
    #     main_map = dp.Plot(main_map)
    #     movement_analysis_html = ""
    #     header_pattern = r'\*\*(.*?)\*\*'
    #     headers = re.findall(header_pattern, movement_analysis)
    #     for header in headers:
    #         placeholder = f'<b>{header}</b>'
    #         movement_analysis = movement_analysis.replace(f'**{header}**', placeholder)
        
    #      # Split the reports by new lines and iterate over them
    #     for report in movement_analysis.split('\n\n'):
    #         # Skip empty lines
    #         if not report.strip():
    #             continue
            
    #             # Split each report into report number and content
    #         report_lines = report.split('\n')
            
    #         report_paragraphs = f"• {report}<br>"
    #         movement_analysis_html+= report_paragraphs
            
    #     movement_analysis = dp.HTML(self.format_text('Analysis of Device Movement',movement_analysis_html))

    #     fig_devices_movments = dp.Plot(fig_devices_movments)
    #     movement_analysis_grouped = dp.Group(blocks=[movement_analysis,fig_devices_movments ],label='Analysis of Device Movement')

    #     threat_analysis_hmtl=''
    #     for report in re.split('[*•#]', threat_analysis):
    #         # Skip empty lines
    #         if not report.strip():
    #             continue
    #         report_paragraphs = f"• {report}<br>"
    #         threat_analysis_hmtl+= report_paragraphs
        
    #     threat_analysis = dp.Group(blocks = [dp.HTML(threat_score_table),dp.HTML(self.format_text('Threat Analysis Score',threat_analysis_hmtl))],label='Threat Analysis Score')
        
    #     patterns_links_html=''
    #     for report in re.split('[*•#]', patterns_links):
    #         # Skip empty lines
    #         if not report.strip():
    #             continue
    #         report_paragraphs = f"• {report}<br>"
    #         patterns_links_html+= report_paragraphs
    #     patterns_links =  dp.HTML(self.format_text('Patterns And Links',patterns_links_html), label= 'Patterns And Links')

    #     movement_analysis_select = dp.Select(blocks=[movement_analysis_grouped,threat_analysis,patterns_links])


    #     # cotraveler_table_title = dp.HTML(self.format_text(title = "Geo Cotravlers",text_bool=False))
    #     cotraveler_table = dp.HTML(cotraveler_table,label = 'Cotravelers Metrics')
    #     # if len(heatmap_plots)> 1:
    #     #     blocks = []
    #     #     for heatmap in heatmap_plots:
    #     #         block = dp.Plot(heatmap)
    #     #         blocks.append(block)
    #     #     cotraveler_heatmap = dp.Select(blocks=blocks,type=dp.SelectType.DROPDOWN,label='Cotravelers Common Locations')
    #     # else:
    #     #     cotraveler_heatmap = dp.Plot(heatmap_plots[0] , label='Cotravelers Common Locations')

    #     cotraveler_barchart_dow_desctription = dp.HTML(self.format_text('Cotravelers DOW',cotraveler_barchart_dow_desctription))
    #     cotraveler_barchart = dp.Plot(cotraveler_barchart)
    #     cotraveler_barchart_analysis = dp.HTML(self.format_text('DOW Analysis',cotraveler_barchart_analysis))
    #     cotraveler_barchart_group = dp.Group(blocks=[cotraveler_barchart_dow_desctription, cotraveler_barchart, cotraveler_barchart_analysis],label='Cotravelers DOW')
        
    #     cotraveler_select = dp.Select(blocks=[cotraveler_barchart_group, cotraveler_table])


    #     aoi_table = dp.HTML(aoi_table,label='Main Areas Of Interest')
    #     # suspiciousness_results
    #     location_likelihood_wrt_dow_fig = dp.Plot(location_likelihood_wrt_dow_fig)
    #     observations=''
    #     for report in re.split('[*•#]', location_likelihood_Observation):
    #         # Skip empty lines
    #         if not report.strip():
    #             continue
    #         report_paragraphs = f"• {report}<br>"
    #         observations+= report_paragraphs

    #     analysis=''
    #     for report in re.split('[*•#]', location_likelihood_analysis):
    #         # Skip empty lines
    #         if not report.strip():
    #             continue
    #         report_paragraphs = f"• {report}<br>"
    #         analysis+= report_paragraphs

    #     observations = dp.HTML(self.format_text('Observations',observations, 2))
    #     analysis = dp.HTML(self.format_text('Analysis',analysis,2))
        
    #     location_likelihood_wrt_dow_description = dp.Group(blocks=[observations,analysis])
    #     location_likelihood_wrt_dow = dp.Group(blocks=[location_likelihood_wrt_dow_fig,location_likelihood_wrt_dow_description],label='Location Likelihood with respect to Day of Week') 
    #     location_duration_wrt_dow_fig = dp.Plot(location_duration_wrt_dow_fig)


    #     location_likelihood_duration_Observation_html =''
    #     for report in re.split('[*•#]', location_likelihood_duration_Observation):
    #         # Skip empty lines
    #         if not report.strip():
    #             continue
    #         report_paragraphs = f"• {report}<br>"
    #         location_likelihood_duration_Observation_html+= report_paragraphs

    #     location_likelihood_duration_analysis_html=''
    #     for report in re.split('[*•#]', location_likelihood_duration_analysis):
    #         # Skip empty lines
    #         if not report.strip():
    #             continue
    #         report_paragraphs = f"• {report}<br>"
    #         location_likelihood_duration_analysis_html+= report_paragraphs

    #     location_likelihood_duration_Observation = dp.HTML(self.format_text('Observations', location_likelihood_duration_Observation_html, 2))
    #     location_likelihood_duration_analysis = dp.HTML(self.format_text('Analysis',location_likelihood_duration_analysis_html, 2 ))
    #     location_duration_wrt_dow_description = dp.Group(blocks=[location_likelihood_duration_Observation,location_likelihood_duration_analysis], widths=[1])
    #     location_duration_wrt_dow = dp.Group(blocks=[location_duration_wrt_dow_fig,location_duration_wrt_dow_description],label='Duration per Location with respect to Day of Week')
    #     # location_duration_wrt_dow_description
        
    #     aoi_select = dp.Select(blocks=[aoi_table, aoi_mapp, location_likelihood_wrt_dow, location_duration_wrt_dow])
    #     # common_location_html=''
    #     # for report in re.split('[*•#]', common_location):
    #     #     # Skip empty lines
    #     #     if not report.strip():
    #     #         continue
    #     #     report_paragraphs = f"• {report}<br>"
    #     #     common_location_html+= report_paragraphs
               
    #     # cotraveler_group = dp.Group(blocks=[cotraveler_table_title,cotraveler_table])
    #     # common_location =  dp.HTML(self.format_text('Common Location',common_location_html))

    #     # common_location_fig = dp.Plot(common_location_fig)
        
    #     # common_location_grouped = dp.Group(blocks=[common_location, common_location_fig], label= 'Common Location')
    #     # device_colocation_html=''
    #     # for report in re.split('[*•#]', device_colocation):
    #     #     # Skip empty lines
    #     #     if not report.strip():
    #     #         continue
    #     #     report_paragraphs = f"• {report}<br>"
    #     #     device_colocation_html+= report_paragraphs
    #     # device_colocation =  dp.HTML(self.format_text('Device Co-location Analysis',device_colocation_html), label= 'Device Co-location Analysis')
    #     # device_colocation_map = dp.Plot(common_location_map)

    #     # device_colocation_grouped = dp.Group(blocks=[device_colocation,device_colocation_map], label = 'Device Co-location Analysis')
    #     # significane_location_html=''
    #     # for report in re.split('[*•#]', significane_location):
    #     #     # Skip empty lines
    #     #     if not report.strip():
    #     #         continue
    #     #     report_paragraphs = f"• {report}<br>"
    #     #     significane_location_html+= report_paragraphs

    #     # significane_location =  dp.HTML(self.format_text('Significance of Locations',significane_location_html), label= 'Significance of Locations')

    #     # common_location_select = dp.Select(blocks=[common_location_grouped,device_colocation_grouped,significane_location])
    #     conclusion =  dp.HTML(self.format_text('Conclusion',conclusion))

    #     report = dp.Blocks(
    #     dp.Page(title="Report",blocks=[ header_valoores, abstract, summary_table, introduction, main_map, movement_analysis_select, cotraveler_select, aoi_select, conclusion]
    #     ),
    #     dp.Page(title="Devices Mapping", blocks=[dp.DataTable(mapping_table)]),
    #     )

    #     return report
    

    def get_simulation_report_layout_dh(self,dh_dictionary:dict=None):

        title = dh_dictionary['title']
        table_id = dh_dictionary['table_id']
        main_map = dh_dictionary['main_map']
        abstract = dh_dictionary['abstract']
        introduction = dh_dictionary['introduction']
        summary_table = dh_dictionary['summary_table']
        movement_analysis = dh_dictionary['movement_analysis']
        threat_analysis = dh_dictionary['threat_analysis']
        threat_score_table = dh_dictionary['threat_score_table']
        fig_devices_movments = dh_dictionary['fig_devices_movments']
        patterns_links = dh_dictionary['patterns_links']
        cotraveler_table = dh_dictionary['cotraveler_table']
        location_likelihood_Observation = dh_dictionary['location_likelihood_Observation']
        location_likelihood_analysis = dh_dictionary['location_likelihood_analysis']
        cotraveler_barchart_dow_desctription = dh_dictionary['cotraveler_barchart_dow_desctription']
        cotraveler_barchart_analysis = dh_dictionary['cotraveler_barchart_analysis']
        cotraveler_barchart = dh_dictionary['cotraveler_barchart']
        heatmap_plots = dh_dictionary['heatmap_plots']
        aoi_table = dh_dictionary['aoi_table']
        aoi_mapp = dh_dictionary['aoi_mapp']
        # suspiciousness_results = dh_dictionary['suspiciousness_results']
        location_likelihood_wrt_dow_fig = dh_dictionary['location_likelihood_wrt_dow_fig']
        location_duration_wrt_dow_fig = dh_dictionary['location_duration_wrt_dow_fig']
        # location_duration_wrt_dow_description = dh_dictionary['location_duration_wrt_dow_description']
        location_likelihood_duration_Observation = dh_dictionary['location_likelihood_duration_Observation']
        location_likelihood_duration_analysis = dh_dictionary['location_likelihood_duration_analysis']
        conclusion = dh_dictionary['conclusion']
        mapping_table = dh_dictionary['mapping_table']
        
        with open(self.properties.report_valoores_png_path, 'rb') as file:
            binary_data = file.read()

            # Encode the binary data to Base64
        base64_encoded = base64.b64encode(binary_data).decode('utf-8')
        header_valoores = dp.HTML(self.properties.report_banner_html.format(title,base64_encoded))
        # header_valoores = dp.Group(dp.Media(self.properties.report_valoores_png_path), dp.HTML(self.properties.report_banner_html.format(title)), columns=2, widths=[1, 12])
        


        abstract = dp.HTML(self.format_text('Key Takeaways and Crucial Findings',abstract))
        html_table = summary_table.to_html(classes='table-style', escape=False,index=False)

        css_set_column_width = self.properties.css_set_column_width

        summary_table = css_set_column_width + html_table
        html_table = threat_score_table.to_html(classes='table-style', escape=False,index=False)
        threat_score_table = css_set_column_width + html_table
        html_table = cotraveler_table.to_html(classes='table-style', escape=False,index=False)
        cotraveler_table = css_set_column_width + html_table
        html_table = aoi_table.to_html(classes='table-style', escape=False,index=False)
        aoi_table = css_set_column_width + html_table
        # summary_table = dp.Table(summary_table)
        summary_table = dp.HTML(summary_table)
        introduction = dp.HTML(self.format_text('Background',introduction))
        main_map = dp.Plot(main_map)
        movement_analysis_html = ""
        header_pattern = r'\*\*(.*?)\*\*'
        headers = re.findall(header_pattern, movement_analysis)
        for header in headers:
            placeholder = f'<b>{header}</b>'
            movement_analysis = movement_analysis.replace(f'**{header}**', placeholder)
        
         # Split the reports by new lines and iterate over them
        for report in movement_analysis.split('\n\n'):
            # Skip empty lines
            if not report.strip():
                continue
            
                # Split each report into report number and content
            report_lines = report.split('\n')
            
            report_paragraphs = f"• {report}<br>"
            movement_analysis_html+= report_paragraphs
            
        movement_analysis = dp.HTML(self.format_text('Analysis of Device Movement',movement_analysis_html))

        fig_devices_movments = dp.Plot(fig_devices_movments)
        movement_analysis_select= dp.Group(blocks=[movement_analysis,fig_devices_movments ],label='Analysis of Device Movement')

        threat_analysis_hmtl=''
        for report in re.split('[*•#]', threat_analysis):
            # Skip empty lines
            if not report.strip():
                continue
            report_paragraphs = f"• {report}<br>"
            threat_analysis_hmtl+= report_paragraphs
        
        threat_analysis = dp.Group(blocks = [dp.HTML(self.format_text('Threat Analysis Score',threat_analysis_hmtl)), dp.HTML(threat_score_table)],label='Threat Analysis Score')
        
        patterns_links_html=''
        for report in re.split('[*•#]', patterns_links):
            # Skip empty lines
            if not report.strip():
                continue
            report_paragraphs = f"• {report}<br>"
            patterns_links_html+= report_paragraphs
        patterns_links =  dp.HTML(self.format_text('Patterns And Links',patterns_links_html), label= 'Patterns And Links')

        threat_analysis_select = dp.Group(blocks=[threat_analysis,patterns_links],label='Threat Analysis')


        # cotraveler_table_title = dp.HTML(self.format_text(title = "Geo Cotravlers",text_bool=False))
        cotraveler_title = dp.HTML(self.format_title('Matrices Table - Cotravelers',header=1))
        cotraveler_table = dp.HTML(cotraveler_table,label = 'Cotravelers Metrics')
        # if len(heatmap_plots)> 1:
        #     blocks = []
        #     for heatmap in heatmap_plots:
        #         block = dp.Plot(heatmap)
        #         blocks.append(block)
        #     cotraveler_heatmap = dp.Select(blocks=blocks,type=dp.SelectType.DROPDOWN,label='Cotravelers Common Locations')
        # else:
        #     cotraveler_heatmap = dp.Plot(heatmap_plots[0] , label='Cotravelers Common Locations')

        cotraveler_barchart_dow_desctription = dp.HTML(self.format_text('Cotravelers DOW',cotraveler_barchart_dow_desctription))
        cotraveler_barchart = dp.Plot(cotraveler_barchart)
        cotraveler_barchart_analysis = dp.HTML(self.format_text('DOW Analysis',cotraveler_barchart_analysis))
        cotraveler_barchart_group = dp.Group(blocks=[cotraveler_barchart_dow_desctription, cotraveler_barchart, cotraveler_barchart_analysis],label='Cotravelers DOW')
        
        cotraveler_select = dp.Group(blocks=[cotraveler_title, cotraveler_table , cotraveler_barchart_group],label='Cotravelers Metrics')

        aoi_title = dp.HTML(self.format_title(title = "Matrices Table - Main Areas Of Interest",header=1))
        aoi_table = dp.HTML(aoi_table,label='Main Areas Of Interest')
        # suspiciousness_results
        location_likelihood_wrt_dow_fig = dp.Plot(location_likelihood_wrt_dow_fig)
        observations=''
        for report in re.split('[*•#]', location_likelihood_Observation):
            # Skip empty lines
            if not report.strip():
                continue
            report_paragraphs = f"• {report}<br>"
            observations+= report_paragraphs

        analysis=''
        for report in re.split('[*•#]', location_likelihood_analysis):
            # Skip empty lines
            if not report.strip():
                continue
            report_paragraphs = f"• {report}<br>"
            analysis+= report_paragraphs

        observations = dp.HTML(self.format_text('Observations',observations, 2))
        analysis = dp.HTML(self.format_text('Analysis',analysis,2))
        
        location_likelihood_wrt_dow_description = dp.Group(blocks=[observations,analysis])
        location_likelihood_wrt_dow = dp.Group(blocks=[location_likelihood_wrt_dow_fig,location_likelihood_wrt_dow_description],label='Location Likelihood with respect to Day of Week') 
        location_duration_wrt_dow_fig = dp.Plot(location_duration_wrt_dow_fig)


        location_likelihood_duration_Observation_html =''
        for report in re.split('[*•#]', location_likelihood_duration_Observation):
            # Skip empty lines
            if not report.strip():
                continue
            report_paragraphs = f"• {report}<br>"
            location_likelihood_duration_Observation_html+= report_paragraphs

        location_likelihood_duration_analysis_html=''
        for report in re.split('[*•#]', location_likelihood_duration_analysis):
            # Skip empty lines
            if not report.strip():
                continue
            report_paragraphs = f"• {report}<br>"
            location_likelihood_duration_analysis_html+= report_paragraphs

        location_likelihood_duration_Observation = dp.HTML(self.format_text('Observations', location_likelihood_duration_Observation_html, 2))
        location_likelihood_duration_analysis = dp.HTML(self.format_text('Analysis',location_likelihood_duration_analysis_html, 2 ))
        location_duration_wrt_dow_description = dp.Group(blocks=[location_likelihood_duration_Observation,location_likelihood_duration_analysis], widths=[1])
        location_duration_wrt_dow = dp.Group(blocks=[location_duration_wrt_dow_fig,location_duration_wrt_dow_description],label='Duration per Location with respect to Day of Week')
        # location_duration_wrt_dow_description
        
        aoi_select = dp.Group(blocks=[aoi_title, aoi_table, aoi_mapp, location_likelihood_wrt_dow, location_duration_wrt_dow],label='Areas Of Interest')
        
        select_all = dp.Select(blocks=[movement_analysis_select, cotraveler_select, aoi_select,threat_analysis_select])
        # common_location_html=''
        # for report in re.split('[*•#]', common_location):
        #     # Skip empty lines
        #     if not report.strip():
        #         continue
        #     report_paragraphs = f"• {report}<br>"
        #     common_location_html+= report_paragraphs
               
        # cotraveler_group = dp.Group(blocks=[cotraveler_table_title,cotraveler_table])
        # common_location =  dp.HTML(self.format_text('Common Location',common_location_html))

        # common_location_fig = dp.Plot(common_location_fig)
        
        # common_location_grouped = dp.Group(blocks=[common_location, common_location_fig], label= 'Common Location')
        # device_colocation_html=''
        # for report in re.split('[*•#]', device_colocation):
        #     # Skip empty lines
        #     if not report.strip():
        #         continue
        #     report_paragraphs = f"• {report}<br>"
        #     device_colocation_html+= report_paragraphs
        # device_colocation =  dp.HTML(self.format_text('Device Co-location Analysis',device_colocation_html), label= 'Device Co-location Analysis')
        # device_colocation_map = dp.Plot(common_location_map)

        # device_colocation_grouped = dp.Group(blocks=[device_colocation,device_colocation_map], label = 'Device Co-location Analysis')
        # significane_location_html=''
        # for report in re.split('[*•#]', significane_location):
        #     # Skip empty lines
        #     if not report.strip():
        #         continue
        #     report_paragraphs = f"• {report}<br>"
        #     significane_location_html+= report_paragraphs

        # significane_location =  dp.HTML(self.format_text('Significance of Locations',significane_location_html), label= 'Significance of Locations')

        # common_location_select = dp.Select(blocks=[common_location_grouped,device_colocation_grouped,significane_location])
        conclusion =  dp.HTML(self.format_text('Conclusion',conclusion))

        report = dp.Blocks(
        dp.Page(title="Report",blocks=[ header_valoores, abstract, summary_table, introduction, main_map, select_all]
        ),
        dp.Page(title="Devices Mapping", blocks=[dp.DataTable(mapping_table)]),
        )

        return report