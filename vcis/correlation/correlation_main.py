import pandas as pd
from vcis.utils.utils import CDR_Utils, CDR_Properties
from vcis.correlation.cdr_correlation_functions import CDRCorrelationFunctions
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.correlation.gps_correlation_functions import GPSCorrelationFunctions
import math

##########################################################################################################

class CorrelationMain():
    def __init__(self, verbose: bool = True):
        self.correlation_functions = CDRCorrelationFunctions(verbose)
        self.utils = CDR_Utils(verbose)
        self.properties = CDR_Properties()
        self.cassandra_tools = CassandraTools(verbose)
        self.verbose = verbose
        self.nodes_functions = GPSCorrelationFunctions(verbose)
    def cdr_correlation(self,
                    imsi_id:str = None,
                    start_date:str = None,
                    end_date:str = None,
                    local:bool = True,
                    distance:int = 50,
                    server:str = '10.1.10.110',
                    region:int = 142,
                    sub_region:int = 145
                    ):
        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Fetch Device Data'.center(29)))
        df_device = self.cassandra_tools.get_device_history_imsi(imsi_id, start_date, end_date, server= server)
        print('★★★',df_device)

        df_device = self.utils.combine_coordinates(df_device,latitude_column=self.properties.location_latitude,longitude_column=self.properties.location_longitude)
        df_device = self.utils.convert_ms_to_datetime(df_device)

        df_visits = self.correlation_functions.add_visit_columns(df_device)
        df_visits = self.correlation_functions.filter_visit_columns(df_visits)

        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Scanning for Nearby'.center(29)))
        df_common = self.correlation_functions.get_common_df(df_visits, scan_distance = distance, passed_server = server, region=region, sub_region=sub_region)
        device_list =self.correlation_functions.get_device_list(df_common)

        # device_list = self.correlation_functions.separate_list(device_list,15)

        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Fetch Nearby History'.center(29)))
        df_history = self.cassandra_tools.get_device_history_geo_chunks(device_list, start_date, end_date, region, sub_region, server)

        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('History Fetch Complete'.center(29)))
        df_history = self.utils.combine_coordinates(df_history,latitude_column=self.properties.location_latitude,longitude_column=self.properties.location_longitude)
        print('History DataFrame:\n',df_history)

        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Adding Percentage'.center(29)))
        table = self.correlation_functions.add_percentage_column(df_history=df_history,df_common=df_common)

        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Adding Visit Count'.center(29)))
        table = self.correlation_functions.add_visit_count_column(table=table,df_common=df_common)
        table = self.correlation_functions.add_common_locations_hits_coulmn(table=table,df_common=df_common)
        table = self.correlation_functions.add_statistics_columns(table = table,df_device = df_device ,df_history=df_history)
        table = table.rename(columns=str.upper)
        try:
            table.drop(['UNNAMED: 0'], axis=1, inplace=True)
        except:
            pass
        
        return table ,df_device
    

    def gps_correlation(self,
                    df_geo:pd.DataFrame = None,
                    device_id_list:list = None,
                    start_date:str = None,
                    end_date:str = None,
                    local:bool = True,
                    distance:int = 30,
                    server:str = '10.1.10.110',
                    region:int = 142,
                    sub_region:int = 145,
                    default_timeout:int = 10000,
                    default_fetch_size:int = 150000,
                    ):
        session = self.cassandra_tools.get_cassandra_connection(server, default_timeout, default_fetch_size)
        if df_geo== None:
            #df_geo = self.cassandra_tools.get_device_history_geo(device_id = device_id , start_date= start_date , end_date= end_date , server=server,region= region, sub_region=sub_region,session=session)
            df_geo = self.cassandra_tools.get_device_history_geo_chunks(device_list_separated= device_id_list,start_date= start_date , end_date= end_date,region= region, sub_region=sub_region,session=session)
        df_geo.to_csv(self.properties.passed_filepath_excel + "df_main")
        if df_geo.empty: 
            print("NO DATA FOUND for {} !!!".format(device_id_list))
            return None
        

        df_geo = self.utils.binning(df_geo,distance)
        df_filtered = df_geo.drop_duplicates(subset=['latitude_grid','longitude_grid'])
        df_filtered = df_filtered.sort_values(['latitude_grid','longitude_grid'])
        grids = df_filtered[['latitude_grid', 'longitude_grid']].to_dict('records')

        step_lat, step_lon = self.utils.get_step_size(distance*math.sqrt(2))
        neighboring_grids = self.nodes_functions.find_neighboring_grids(grids, step_lat, step_lon)
        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Neighboring Grids Found'.center(50)))
        polygons_list = self.nodes_functions.get_polygon_list(neighboring_grids, step_lat)
        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Polygon List Created'.center(50)))
        neighboring_grids_df = self.nodes_functions.convert_polygon_to_df(neighboring_grids, polygons_list)
        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Polygon DataFrame Conversion Done'.center(50)))
        
        df_geo['latitude_grid'] = df_geo['latitude_grid'].round(6) 
        df_geo['longitude_grid'] = df_geo['longitude_grid'].round(6)

        df_geo = self.utils.combine_coordinates(df_geo)
        df_near_nodes = self.nodes_functions.get_close_nodes(data=neighboring_grids_df,session=session)
        print(df_near_nodes)
        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<29} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Close Nodes Data Retrieval Complete'.center(50)))
        df_near_nodes.to_csv(self.properties.passed_filepath_excel +'df_near_nodes.csv')

        
        ############## TESTED IN STEP ONE ########################

        # df_near_nodes.to_csv('df_near_nodes.csv',index=False)
        # # df_near_nodes = pd.read_csv('df_near_nodes.csv')

        # df_near_nodes['osmid'] = df_near_nodes['osmid'].astype(str)
        # df_gps_nodes = self.nodes_functions.get_possible_cgi_id(df_near_nodes,session=session,server = server)
        # df_gps_nodes.to_csv('df_gps_nodes.csv',index=False)
        
        # # df_gps_nodes = pd.read_csv('df_gps_nodes.csv')

        # print(df_gps_nodes)
        # df_gps_nodes = self.nodes_functions.get_bts_location(df_gps_nodes,server = server,session=session)

        # df_gps_nodes['bts_azimuth'] = df_gps_nodes['bts_azimuth'].replace(to_replace=r'(?i)indoor', value=360, regex=True).astype(float)
        # df_gps_nodes['bts_latitude'] = df_gps_nodes['bts_latitude'].astype(float)
        # df_gps_nodes['bts_longitude'] = df_gps_nodes['bts_longitude'].astype(float)
    
        # df_connected_devices = self.nodes_functions.get_connected_devices(df_gps_nodes,session=session,server= server)
        # df_connected_devices = df_connected_devices.drop_duplicates()


        # groups = df_connected_devices.groupby(['imsi_id','imei_id','phone_number']).size().reset_index(name='counts')

        # top_id = groups.loc[groups['counts'].idxmax()]
        # top_id = {key: value for key, value in top_id.items()}
        # top_id = pd.DataFrame([top_id])
        top_id=0
        # TO BE CONTINUED
        return top_id

        ############### TESTED IN STEP TWO ######################## 
        # grouped = df_gps_nodes.groupby(['gps_latitude','gps_longitude','usage_timeframe'])

        # count=0
        # for name, group in grouped:

        #     gps_trace = go.Scattermapbox(
        #         lat=group['gps_latitude'],
        #         lon=group['gps_longitude'],
        #         mode='markers',
        #         marker=dict(size=10),
        #         name='GPS Points',
        #         legendgroup='group1',
        #         visible='legendonly'
        #     )

        #     bts_trace = go.Scattermapbox(
        #         lat=group['bts_latitude'],
        #         lon=group['bts_longitude'],
        #         mode='markers',
        #         marker=dict(size=6),
        #         name='BTS Points',
        #         legendgroup='group1',
        #         visible='legendonly'
        #     )
        #     latitude = group['bts_latitude'].head(1).iloc[0]
        #     longitude = group['bts_longitude'].head(1).iloc[0] 
        #     azimuth = group['bts_azimuth'].head(1).iloc[0]
            
        #     triangle_coordinates = utils.calculate_sector_triangle(latitude, longitude, azimuth)
        #     x_values,y_values = split_coordinates(triangle_coordinates)
        #     bts_polygon = go.Scatter(
        #     x=x_values,
        #     y=y_values,
        #     mode='lines+markers',  # Draw lines between points and markers for each vertex
        #     marker=dict(size=8),
        #     line=dict(width=2),
        #     fill='toself',  # Close the shape to form a filled triangle
        #     fillcolor='rgba(255,  0,  0,  0.5)'  # Semi-transparent red
        #     )

        #     layout = go.Layout(
        #         autosize=True,
        #         hovermode='closest',
        #         mapbox=dict(
        #             bearing=0,
        #             center=dict(lat=float(group['bts_latitude'].head(1).iloc[0]), lon=float(group['bts_latitude'].head(1).iloc[0])),  # Center of the map
        #             pitch=0,
        #             zoom=10
        #         ),
        #         showlegend=True,
        #         legend=dict(
        #             x=0,
        #             y=1,
        #             traceorder='normal',
        #             font=dict(family='sans-serif', size=12, color='black'),
        #             bgcolor='rgba(255,  255,  255,  0.8)',
        #             bordercolor='White',
        #             borderwidth=1
        #         )
        #     )
        #     fig = go.Figure(data=[gps_trace, bts_trace], layout=layout)

        #     fig.update_layout(mapbox_style="open-street-map")
        #     fig.write_html(properties.passed_filepath_data + "step_two/" +f"map_{count}.html")
        #     count+=1

        # df_gps_nodes.to_csv(properties.passed_filepath_excel + 'df_gps_nodes.csv',index=False)
        # df_gps_nodes = pd.read_csv(properties.passed_filepath_excel + 'df_gps_nodes.csv')
        # plots.bts_nodes_cgi_plot(df_gps_nodes)
