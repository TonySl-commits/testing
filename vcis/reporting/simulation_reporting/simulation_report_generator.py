##############################################################################################################################
    # Imports
##############################################################################################################################

import plotly.express as px
import plotly.figure_factory as ff
from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.reporting.report_main.reporting_tools import ReportingTools
from vcis.reporting.simulation_reporting.simulation_report_functions import SimulationReportFunctions
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.plots.plots_tools import PlotsTools
from vcis.confidence_analysis.geo_confidence_analysis import GeospatialConfidenceAnalysis

class SimulationReportGenerator():
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.properties = CDR_Properties()
        self.utils = CDR_Utils(verbose=self.verbose)
        self.cassandra_tools = CassandraTools()
        self.cassandra_spark_tools = CassandraSparkTools()
        self.reporting_tools = ReportingTools(self.verbose)
        self.simulation_report_functions = SimulationReportFunctions()
        self.oracle_tools = OracleTools()
        self.plot_tools = PlotsTools()
        
##############################################################################################################################
    # Reporting Functions
##############################################################################################################################
    def get_main_map(self, report_table,table_id):
        table = report_table.copy()
        table.columns = table.columns.str.lower()
        shapes = self.oracle_tools.get_simulation_shapes(table_id=table_id)
        main_map = self.plot_tools.get_simulation_plot(data=table, shapes = shapes)
        return main_map
    
    def get_confidence_analysis(self, df_history,table_id):
        table = df_history.copy()
        table.columns = table.columns.str.lower()
        confidence_analysis = GeospatialConfidenceAnalysis()
        confidence_analysis_df = confidence_analysis.evaluate_confidence_in_analysis_wrt_service_provider(table,table_id)
        return confidence_analysis_df

    def get_number_of_hits_per_server_provider(self, report_table):
        fig = self.simulation_report_functions.hits_per_service_provider_id(report_table)
        
        figure_block = {
            'BLOCK_CONTENT': fig,
            'BLOCK_NAME': 'Number of Hits per Service Provider',
            'BLOCK_TYPE': 'PLOTLY',
            'BLOCK_ASSOCIATION': 'Simulation',
            'DEVICE_ID': 'id'
        }

        return figure_block
    
    def get_number_of_hits_per_dow(self,report_table):
        fig = self.simulation_report_functions.number_of_hits_per_dow(report_table)

        figure_block = {
            'BLOCK_CONTENT': fig,
            'BLOCK_NAME': 'Number of Hits per Day of Week',
            'BLOCK_TYPE': 'PLOTLY',
            'BLOCK_ASSOCIATION': 'Simulation',
            'DEVICE_ID': 'id'
        }
        return figure_block

    def get_number_of_hits_per_month(self,report_table):
        fig = self.simulation_report_functions.number_of_hits_per_month(report_table)

        figure_block = {
            'BLOCK_CONTENT': fig,
            'BLOCK_NAME': 'Number of Hits per Month',
            'BLOCK_TYPE': 'PLOTLY',
            'BLOCK_ASSOCIATION': 'Simulation',
            'DEVICE_ID': 'id'
        }

        return figure_block 
    
    def get_number_of_hits_per_hod(self,report_table):
        fig = self.simulation_report_functions.number_of_hits_per_hod(report_table)
        
        figure_block = {
            'BLOCK_CONTENT': fig,
            'BLOCK_NAME': 'Number of Hits per Hour of Day',
            'BLOCK_TYPE': 'PLOTLY',
            'BLOCK_ASSOCIATION': 'Simulation',
            'DEVICE_ID': 'id'
        }

        return figure_block 
    
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


            summary_stats.columns = ['Device ID', 'Number of Hits', 'Number of Days','Number Of Countries','Number Of Cities','Country list','City list']
        
        print('INFO:    SUMMARY STATISTICS\n', summary_stats)

        summary_statistics_block = {
            'BLOCK_CONTENT': summary_stats,
            'BLOCK_NAME': 'Summary Statistics',
            'BLOCK_TYPE': 'table',
            'BLOCK_ASSOCIATION': 'Simulation',
            'DEVICE_ID': 'id'
        }
        return summary_statistics_block
    
    def get_number_of_hits_per_cgi(self,report_table):
        hits_per_cgi_id_type = report_table.groupby(['CGI_ID', 'TYPE_ID']).size().reset_index(name='NumberOfHits')

        # Create the Icicle Chart
        fig = px.icicle(hits_per_cgi_id_type, path=['TYPE_ID', 'CGI_ID'], values='NumberOfHits')
        fig.update_traces(root_color="lightgrey")
        fig.update_layout(margin=dict(t=50, l=25, r=25, b=25))
        
        figure_block = {
            'BLOCK_CONTENT': fig,
            'BLOCK_NAME': 'Number of Hits per CGI ID',
            'BLOCK_TYPE': 'PLOTLY',
            'BLOCK_ASSOCIATION': 'Simulation',
            'DEVICE_ID': 'id'
        }
        
        return figure_block
