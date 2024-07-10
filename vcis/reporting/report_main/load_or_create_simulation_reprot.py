
import plotly.graph_objs as go
import plotly.io as pio
import pandas as pd
import numpy as np

from vcis.reporting.report_main.reporting_tools import ReportingTools
from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.reporting.report_main.report_generator import ReportGenerator
from vcis.reporting.simulation_reporting.simulation_report_generator import SimulationReportGenerator


class LoadOrCreateSimulationReport:
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.properties = CDR_Properties()
        self.utils = CDR_Utils(verbose=self.verbose)
        self.oracle_tools = OracleTools()
        self.reporting_tools = ReportingTools(self.verbose)
        self.simulation_report_generator = SimulationReportGenerator()
        self.report = ReportGenerator()

    def load_simulation_data(self, table_id, report_type):
        query = f"SELECT * FROM locdba.loc_report_config where LOC_REPORT_CONFIG_ID ={table_id} "

        config_table= self.oracle_tools.get_oracle_query(query)

        report_type = config_table['LOC_REPORT_TYPE'][0]
        if report_type==11:
            oracle_table = self.oracle_tools.get_table("SSDX_ENG","DATACROWD_CDR_DATA",table_id,True)
        else:    
            oracle_table = self.oracle_tools.get_table("SSDX_ENG","TMP_REPORT_COORDINATES_6",table_id,True)


        query = f"SELECT REPORT_TYPE , REPORT_NAME FROM TECHDBA.CYB_REPORT_HTML_TEMPLATES where REPORT_TYPE={report_type}"
        table_type = self.oracle_tools.get_oracle_query(query)
        report_name = table_type['REPORT_NAME'][0]
        return config_table, oracle_table, report_name, report_type
    def check_simulation_blocks_exists(self, table_id):
         try:
            table = self.oracle_tools.get_table("SSDX_TMP","Analytics_Simulation_Blocks",table_id,True)
         except:
            return False
         if table[table['BLOCK_ASSOCIATION']=='Simulation'].empty:
             return False
         else:
             return True



    def load_or_create_simulation_blocks(self, table_id,config_table,oracle_table, report_type,report_name):
        blocks_dictionary = {}
        blocks_aoi_dictionary = {}
        blocks_cotravelers_dictionary = {}
        check_table = self.check_simulation_blocks_exists(table_id=table_id)
        if check_table:
            print("check_table - !!!!!!!!!!!!!!!!!!!!!",check_table)
            table = self.reporting_tools.get_report_blocks_table(table_id=table_id)
        else:
            print("check_table - !!!!!!!!!!!!!!!!!!!!!",check_table)

            self.report.get_simulation_report(main_table = config_table , report_table = oracle_table,table_id = table_id , file_name ='simulation_report',report_type=report_type,report_name=report_name)
            table = self.reporting_tools.get_report_blocks_table(table_id=table_id)

        # Get Simulation Blocks
        print(table)
        number_of_hits_per_service_provider = table[table['BLOCK_NAME']=='Number of Hits per Service Provider']['BLOCK_CONTENT'].values[0]
        number_of_hits_per_service_provider = pio.from_json(number_of_hits_per_service_provider, output_type='Figure')
        blocks_dictionary['number_of_hits_server_providers'] = number_of_hits_per_service_provider

        number_of_hits_per_dow = table[table['BLOCK_NAME']=='Number of Hits per Day of Week']['BLOCK_CONTENT'].values[0]
        number_of_hits_per_dow =  pio.from_json(number_of_hits_per_dow, output_type='Figure')
        blocks_dictionary['number_of_hits_per_dow'] = number_of_hits_per_dow

        number_of_hits_per_month = table[table['BLOCK_NAME']=='Number of Hits per Month']['BLOCK_CONTENT'].values[0]
        number_of_hits_per_month =  pio.from_json(number_of_hits_per_month, output_type='Figure')
        blocks_dictionary['number_of_hits_per_month'] = number_of_hits_per_month

        number_of_hits_per_hod = table[table['BLOCK_NAME']=='Number of Hits per Hour of Day']['BLOCK_CONTENT'].values[0]
        number_of_hits_per_hod =  pio.from_json(number_of_hits_per_hod, output_type='Figure')
        blocks_dictionary['number_of_hits_per_hod'] = number_of_hits_per_hod

        summary_statistics = table[table['BLOCK_NAME']=='Summary Statistics']['BLOCK_CONTENT'].values[0]
        summary_statistics =  pd.read_json(summary_statistics, orient='records')
        blocks_dictionary['summary_statistics'] = summary_statistics
        # print(summary_statistics)

        timeline = table[table['BLOCK_NAME']=='Timeline']['BLOCK_CONTENT'].values[0]
        blocks_dictionary['timeline'] = timeline
        # df = pd.DataFrame(summary_statistics)
        main_map = self.simulation_report_generator.get_main_map(oracle_table,table_id)
        blocks_dictionary["main_map"] = main_map
        aoi_table = table[table['BLOCK_ASSOCIATION']=='AOI']
        if len(aoi_table)!=0:
            unique_device = aoi_table[~(aoi_table['DEVICE_ID'].isin(['null', None,'id']))]['DEVICE_ID'].unique().tolist()
            for device_id in unique_device:
                aoi_device_blocks_dictionary = {}

                df_device = table[table['DEVICE_ID']==device_id]
                print(df_device[df_device['BLOCK_NAME']=='Gauge Chart']['BLOCK_CONTENT'])
                # Confidence Metrics
                gauge_chart = df_device[df_device['BLOCK_NAME']=='Gauge Chart']['BLOCK_CONTENT'].values[0]
                gauge_chart =  pio.from_json(gauge_chart, output_type='Figure')

                confidence_metrics = df_device[df_device['BLOCK_NAME']=='Confidence Metrics']['BLOCK_CONTENT'].values[0]
                confidence_metrics = pio.from_json(confidence_metrics, output_type='Figure')

                confidence_score_explanation = df_device[df_device['BLOCK_NAME']=='Confidence Score Description']['BLOCK_CONTENT'].values[0]
                metrics_explanation = df_device[df_device['BLOCK_NAME']=='Confidence Metrics Explanation']['BLOCK_CONTENT'].values[0]
                
                aoi_device_blocks_dictionary['gauge'] = gauge_chart
                aoi_device_blocks_dictionary['confidence_metrics'] = confidence_metrics
                aoi_device_blocks_dictionary['confidence_score_explanation'] = confidence_score_explanation
                aoi_device_blocks_dictionary['metrics_explanation'] = metrics_explanation

                # AOI Metrics
                hits_per_dow = df_device[df_device['BLOCK_NAME']=='Number of Hits Per Day of Week']['BLOCK_CONTENT'].values[0]
                hits_per_dow =  pio.from_json(hits_per_dow, output_type='Figure')
                dow_description = df_device[df_device['BLOCK_NAME']=='Number of Hits Per Day of Week Report Description']['BLOCK_CONTENT'].values[0]

                hits_per_month = df_device[df_device['BLOCK_NAME']=='Number of Hits Per Month']['BLOCK_CONTENT'].values[0]
                hits_per_month =  pio.from_json(hits_per_month, output_type='Figure')
                month_description = df_device[df_device['BLOCK_NAME']=='Number of Hits per Month Report Description']['BLOCK_CONTENT'].values[0]

                likelihood_dow = df_device[df_device['BLOCK_NAME']=='Location Likelihood Per Day of Week']['BLOCK_CONTENT'].values[0]
                likelihood_dow =  pio.from_json(likelihood_dow, output_type='Figure')
                likelihood_dow_description = df_device[df_device['BLOCK_NAME']=='Location Likelihood Per Day of Week Description']['BLOCK_CONTENT'].values[0]

                duration_dow = df_device[df_device['BLOCK_NAME']=='Location Duration Per Day of Week']['BLOCK_CONTENT'].values[0]
                duration_dow =  pio.from_json(duration_dow, output_type='Figure')
                duration_dow_description = df_device[df_device['BLOCK_NAME']=='Location Duration Per Day of Week Description']['BLOCK_CONTENT'].values[0]

                aoi_device_blocks_dictionary['hits_per_dow'] = hits_per_dow
                aoi_device_blocks_dictionary['hits_per_dow_description'] = dow_description
                aoi_device_blocks_dictionary['hits_per_month'] = hits_per_month
                aoi_device_blocks_dictionary['hits_per_month_description'] = month_description
                aoi_device_blocks_dictionary['lilkelihood_dow'] = likelihood_dow
                aoi_device_blocks_dictionary['lilkelihood_dow_description'] = likelihood_dow_description
                aoi_device_blocks_dictionary['duration_dow'] = duration_dow
                aoi_device_blocks_dictionary['duration_dow_description'] = duration_dow_description

                blocks_aoi_dictionary[device_id] = aoi_device_blocks_dictionary

        if len(table[table['BLOCK_ASSOCIATION']=='COTRAVELER']!=0):
            cotraveler_table = table[table['BLOCK_ASSOCIATION']=='COTRAVELER']
            unique_device = cotraveler_table[~(cotraveler_table['DEVICE_ID'].isin(['null', None,'nan','id']))]['DEVICE_ID'].unique().tolist()
            blocks_cotravelers_dictionary = {}
            for device_id in unique_device:
                df_device = cotraveler_table[cotraveler_table['DEVICE_ID']==device_id]
                blocks_cotravelers_dictionary[device_id] = {}
                pattern = r"Cotraveler KPIs - (.+)"
                cotraveler_ids = df_device['BLOCK_NAME'].str.extract(pattern).squeeze().tolist()
                cotraveler_ids = [x for x in cotraveler_ids if str(x) != 'nan']
                for cotraveler_id in cotraveler_ids:
                    blocks_cotravelers_dictionary[device_id][cotraveler_id] = {}

                    cotraveler_kpis = cotraveler_table[cotraveler_table['BLOCK_NAME']==f'Cotraveler KPIs - {cotraveler_id}']['BLOCK_CONTENT'].values[0]
                    blocks_cotravelers_dictionary[device_id][cotraveler_id][f'Cotraveler KPIs'] = cotraveler_kpis

                    common_areas_plot = cotraveler_table[cotraveler_table['BLOCK_NAME']==f'Common Areas Plot - {cotraveler_id}']['BLOCK_CONTENT'].values[0]
                    common_areas_plot = pio.from_json(common_areas_plot, output_type='Figure')
                    blocks_cotravelers_dictionary[device_id][cotraveler_id][f'Common Areas Plot'] = common_areas_plot

                    number_of_hits_per_dow = cotraveler_table[cotraveler_table['BLOCK_NAME']==f'Number of Hits Per Week - {cotraveler_id}']['BLOCK_CONTENT'].values[0]
                    number_of_hits_per_dow = pio.from_json(number_of_hits_per_dow, output_type='Figure')
                    blocks_cotravelers_dictionary[device_id][cotraveler_id][f'Number of Hits Per Day of Week'] = number_of_hits_per_dow

                    number_of_hits_per_month = cotraveler_table[cotraveler_table['BLOCK_NAME']==f'Number of Hits Per Month - {cotraveler_id}']['BLOCK_CONTENT'].values[0]
                    number_of_hits_per_month = pio.from_json(number_of_hits_per_month, output_type='Figure')
                    blocks_cotravelers_dictionary[device_id][cotraveler_id][f'Number of Hits Per Month'] = number_of_hits_per_month

                # cotraveler_device_blocks_dictionary['cotraveler'] = df_device
                # blocks_cotravelers_dictionary[device_id] = cotraveler_device_blocks_dictionary
        return blocks_dictionary,blocks_aoi_dictionary,blocks_cotravelers_dictionary
    

    def load_or_create_simulation_report(self, table_id,file_name:str ='simulation_report', report_type:int=None):

        config_table, oracle_table, report_name, report_type = self.load_simulation_data(table_id, report_type)
        blocks_dictionary,blocks_aoi_dictionary,blocks_cotravelers_dictionary = self.load_or_create_simulation_blocks(table_id,config_table, oracle_table,report_type,report_name)

        report,report_intoduction = self.reporting_tools.get_simulation_report_layout(main_table = config_table,report_table = oracle_table,report_name=report_name,report_type=report_type ,blocks=blocks_dictionary,table_id=table_id, blocks_aoi_dictionary=blocks_aoi_dictionary, blocks_cotravelers_dictionary=blocks_cotravelers_dictionary)

        self.reporting_tools.save_report(report = report ,report_path = self.properties.passed_filepath_reports_html,open=True,report_name=self.properties.passed_report_name_html_simulation)

        script_contents = self.reporting_tools.get_scripts(self.properties.passed_filepath_reports_html+self.properties.passed_report_name_html_simulation+'.html')
        
        self.oracle_tools.drop_create_table(table_name = self.properties.oracle_analytcis_report_table ,table_id = table_id , table_schema_query = self.properties._oracle_table_schema_query_analytics_report)
         
        self.reporting_tools.insert_into_database(file_name , script_contents,table_id)
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Rporting : Simulation Report Complete'))
