import pandas as pd
from vcis.utils.utils import CDR_Utils, CDR_Properties
from vcis.cotraveler.cotraveler_functions import CotravelerFunctions
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.plots.cotraveler_plots import CotravelerPlots

##########################################################################################################

class CotravelerMain():
    def __init__(self, verbose: bool = False):
        self.cotraveler_functions = CotravelerFunctions()
        self.utils = CDR_Utils(verbose)
        self.properties = CDR_Properties()
        self.cassandra_tools = CassandraTools(verbose)
        self.cassandra_spark_tools = CassandraSparkTools()
        self.cotraveler_plots = CotravelerPlots(verbose)

        self.verbose = verbose

    def cotraveler(self,
                device_id:str,
                start_date:int,
                end_date:int,
                local:bool = True,
                region:int = 142,
                sub_region:int = 145,
                distance:int = 50,
                server:str = '10.10.10.101'
                ):
        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Fetch Device Data'.center(50)))
        session = self.cassandra_tools.get_cassandra_connection(server=server)
        df_main = self.cassandra_tools.get_device_history_geo(device_id=device_id, start_date=start_date, end_date=end_date,region = region,sub_region = sub_region,session=session)
        df_main = df_main.drop_duplicates(subset=['device_id','location_latitude','location_longitude','usage_timeframe'])
        df_main = self.utils.binning(df_main,distance)
        # self.cotraveler_plots.plot_binning_example()
        # self.cotraveler_plots.plot_binning(df_main)
        # self.cotraveler_plots.plot_scan_circles(df_main)
        df_main = self.utils.combine_coordinates(df_main)
        df_main = self.utils.convert_ms_to_datetime(df_main)
        df_main.to_csv(self.properties.passed_filepath_excel+'df_main.csv',index=False)

        df_visits = self.cotraveler_functions.add_visit_columns(df_main)
        df_visits = self.cotraveler_functions.filter_visit_columns(df_visits)

        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Scanning for Nearby'.center(50)))

        df_common = self.cotraveler_functions.get_common_df(df_visits,scan_distance=distance,region=region,sub_region=sub_region,passed_server = server,session=session)
        df_common.to_csv(self.properties.passed_filepath_excel+'df_common.csv',index=False)

        device_list = self.cotraveler_functions.get_device_list(df_common)

        # device_list = self.cotraveler_functions.separate_list(device_list,15)

        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Fetch Nearby History'.center(50)))

        df_history = self.cassandra_tools.get_device_history_geo_chunks(device_list, start_date, end_date,region , sub_region , server,session)
        print(df_history)
        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('History Fetch Complete'.center(50)))
        df_history,step_lat,step_lon = self.utils.binning(df_history,distance,ret=True)
        df_history = self.utils.combine_coordinates(df_history)
        df_history = self.utils.add_reverseGeocode_columns(df_history)
        df_history.to_csv(self.properties.passed_filepath_excel+'df_history.csv',index=False)


        # df_main = pd.read_csv(self.properties.passed_filepath_excel+'df_device.csv')
        # df_history = pd.read_csv(self.properties.passed_filepath_excel+'df_history.csv')
        # df_common = pd.read_csv(self.properties.passed_filepath_excel+'df_common.csv')

        df_main  = self.cotraveler_functions.add_existing_binning(df_main,df_history)

        df_main = self.utils.add_reverseGeocode_columns(df_main)

        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Adding Percentage'.center(50)))

        table = self.cotraveler_functions.add_hits_count_column(df_history=df_history)

        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Adding Visit Count'.center(50)))
        
        table = self.cotraveler_functions.add_visit_count_column(table=table,df_common=df_common)
        table = self.cotraveler_functions.add_common_locations_hits_coulmn(table=table,df_common=df_common)
        table ,df_merged_devices = self.cotraveler_functions.add_statistics_columns(table = table,df_device=df_main,df_history = df_history)
        df_merged_devices.to_csv(self.properties.passed_filepath_excel + 'df_merged_devices.csv')
        total_merged_df = self.cotraveler_functions.get_total_merged_df(df_merged_devices)
        table = self.cotraveler_functions.add_percentage_column(table=table,df_merged_devices=df_merged_devices,total_merged_df=total_merged_df,distance=distance)
        df_common.reset_index(drop=True,inplace=True)

        # df_common_visits = self.utils.get_visit_df(df_common,step_lat=step_lat,step_lon=step_lon)

        # table = self.cotraveler_functions.add_common_timespent_column(table,df_common_visits)
        df_uncommon = self.cotraveler_functions.get_uncommon_df(df_merged_devices,distance=distance)
        table = self.cotraveler_functions.add_uncommon_locations_hits_coulmn(table=table,df_uncommon=df_uncommon)
        table = self.cotraveler_functions.add_uncommon_ratio_column(table=table,total_merged_df=total_merged_df)

        #Timespent
        table = self.cotraveler_functions.add_uncommon_timespent_column(table=table,df_merged_devices=df_merged_devices,distance=distance)

        table = table.rename(columns=str.upper)
        table = table[table['DEVICE_ID'] != df_main['device_id'].iloc[0]]
        table = self.cotraveler_functions.add_classification_column(table)

        df_common = self.utils.convert_ms_to_datetime(df_common)
        heatmap_plots = []

        for device_id in df_common['device_id'].unique():
            df_device = df_history[df_history['device_id'] == device_id]
            df_device_common = df_common[df_common['device_id'] == device_id]
            # heatmap_plot = self.cotraveler_plots.plot_common_heatmap(df_device=df_device,df_device_common=df_device_common,df_main=df_main)
            # heatmap_plots.append(heatmap_plot)

        cotraveler_user_prompt,cotraveler_barchart = self.cotraveler_functions.generate_user_input_and_barchart(df_main,df_history,df_common)

        print(cotraveler_user_prompt)
        # print(table['COMMON_CITIES'])
        try:
            table.drop(['UNNAMED: 0'], axis=1, inplace=True)
        except:
            pass

        return table , df_merged_devices ,df_device ,df_common,df_history, distance,heatmap_plots,cotraveler_barchart,cotraveler_user_prompt
