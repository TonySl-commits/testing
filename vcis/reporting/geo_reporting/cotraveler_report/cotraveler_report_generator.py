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

from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.reporting.report_main.reporting_tools import ReportingTools
from vcis.reporting.geo_reporting.cotraveler_report.cotraveler_report_functions import CotravelerReportFunctions

class CotravelerReportGenerator():
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.properties = CDR_Properties()
        self.utils = CDR_Utils(verbose=self.verbose)
        self.cassandra_tools = CassandraTools()
        self.cassandra_spark_tools = CassandraSparkTools()
        self.reporting_tools = ReportingTools(self.verbose)
        self.cotraveler_report_functions = CotravelerReportFunctions(self.verbose)
        self.title_font = dict(family="Bahnschrift SemiBold", size=20, color='#03045E')
        
##############################################################################################################################
    # Reporting Functions
##############################################################################################################################

    def get_Mapping_Shared_Instances_Across_Time(self,df,table,distance = 50):
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : Mapping Shared Instances Across Time Under Progress'))

        unique_device_ids = table['DEVICE_ID']
        fig_list = []   
        # Create subplots
        # fig = make_subplots(rows=len(unique_device_ids), cols=1, subplot_titles=[f'Device {device_id}' for device_id in unique_device_ids])

        for device_id in unique_device_ids:
            traces = []
            df_device = df[df['device_id_y'] == device_id]
            df_equal = df_device[df_device['grid_x'] == df_device['grid_y']]
            df_not_equal = df_device[df_device['grid_x'] != df_device['grid_y']]
            
            grouped_equal = df_equal.groupby('grid_x').agg({
                'latitude_grid_x': 'first',  
                'longitude_grid_x': 'first',
            }).reset_index()

            grouped_equal.rename(columns={'grid_x': 'grid_x_unique'}, inplace=True)

            grouped_equal['counts'] = df_equal.groupby('grid_x').size().values
            try:
                m = folium.Map(location=[grouped_equal['latitude_grid_x'].mean(),grouped_equal['longitude_grid_x'].mean()], zoom_start=15)
            except:
                m = folium.Map(location=[0,0], zoom_start=15)
            mean_count = grouped_equal['counts'].mean()

            for i, row in grouped_equal.iterrows():
                normalized_count = row['counts'] / mean_count
                
                blue = int(255 * (1 - normalized_count))
                red = int(255 * normalized_count)
                fill_color = f'#{red:02x}00{blue:02x}'
                
                folium.Circle(
                    location=[row['latitude_grid_x'], row['longitude_grid_x']],
                    popup=f'Grid : {row["grid_x_unique"]} , Count : {row["counts"]}',
                    radius=distance * math.sqrt(2), 
                    color="#3186cc",
                    fill=True,
                    fill_color=fill_color,
                    fill_opacity=0.5,
                ).add_to(m)
            m.add_child(folium.LatLngPopup())

            folium.plugins.Geocoder().add_to(m)
            m.add_child(MeasureControl())
            MousePosition().add_to(m)
            fig_list.append(m)
            # m.save(f"shared_instances_mapping_{i}.html")      
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : Mapping Shared Instances Across Time Done'))

        return fig_list
##############################################################################################################################

    def get_time_spent_percentage(self,df,df_main,table,df_history):

        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : Time Spent Under Progress'))
        dict_blocks = []
        unique_device_ids = table['DEVICE_ID']
        df = self.utils.convert_ms_to_datetime(df)

        # df_main = self.utils.convert_ms_to_datetime(df_main)
        df_history = self.utils.convert_ms_to_datetime(df_history)
        # # Create subplots
        fig_main_list = []
        fig_cotraveler_list = []
        time_spent_together_list = []
        time_spent_total_main_list = []
        time_spent_not_together_list = []
        time_spent_total_history_list = []

        for i, device_id in enumerate(unique_device_ids, start=1):
            traces = []
            df_device = df[df['device_id_y'] == device_id]
            df_history_filter = df_history[df_history['device_id'] == device_id]
            df_device_equal = df_device[df_device['grid_x'] == df_device['grid_y']]
        
            time_spent_together = self.cotraveler_report_functions.calculate_time_spent(df_device_equal)
            time_spent_total_main = self.cotraveler_report_functions.calculate_time_spent(df_main,'grid')
            time_spent_total_history = self.cotraveler_report_functions.calculate_time_spent(df_history_filter,'grid')
            time_spent_not_together = time_spent_total_main - time_spent_together
            
            time_spent_not_together_list.append(time_spent_not_together)
            time_spent_together_list.append(time_spent_together)
            time_spent_total_main_list.append(time_spent_total_main)
            time_spent_total_history_list.append(time_spent_total_history)
        df_time_spent = pd.DataFrame({'device_id':unique_device_ids,'time_spent_together':time_spent_together_list,'time_spent_not_together':time_spent_not_together_list,'time_spent_main_total':time_spent_total_main_list,'time_spent_cotraveler_total':time_spent_total_history_list})
        # df_time_spent.to_csv('time_spent.csv')
        df_time_spent['percentage_main'] = (df_time_spent['time_spent_together'] / df_time_spent['time_spent_main_total']) 
        df_time_spent['percentage_cotraveler'] = (df_time_spent['time_spent_together'] / df_time_spent['time_spent_cotraveler_total']) 

        for i, device_id in enumerate(unique_device_ids, start=1):
            percentage = df_time_spent[df_time_spent['device_id'] == device_id]
            percentage.reset_index(drop=True, inplace=True)
            sub_fig_main = self.cotraveler_report_functions.gauge_chart(percentage['percentage_main'][0],device_id,"Time Spent Percentage With Respect TO Main Device")
            sub_fig_cotraveler = self.cotraveler_report_functions.gauge_chart(percentage['percentage_cotraveler'][0],device_id,"Time Spent Percentage With Respect TO Cotraveler")
        #     traces.extend([sub_fig])
        # # buttons = []
        #     fig = go.Figure(data=traces)

            fig_main_list.append(sub_fig_main)
            fig_cotraveler_list.append(sub_fig_cotraveler)

        
        # Add "Select Month" as the first option in the dropdown
        # dropdown_menu = [{"label": "Select Device", "method": "relayout", "args": ["title", 'Mapping Shared Instances Across Time']}]

        # for i, device_id in enumerate(unique_device_ids, start=1):
        #     is_visible = [False] * len(fig.data)
        #     indices = [i for i, trace in enumerate(fig.data) if f'device_id {device_id}' in trace.name]
        #     for idx in indices:
        #         is_visible[idx] = True

        #     button = dict(label=str(device_id),
        #                 method='update',
        #                 args=[{'visible': is_visible}])
        #     buttons.append(button)

        # fig.update_layout(
        #     updatemenus=[
        #         {
        #             'buttons': buttons,
        #             'direction': 'down',
        #             'showactive': True,
        #             'x': 0.01,
        #             'xanchor': 'left',
        #             'y': 1.15,
        #             'yanchor': 'top'
        #         }
        #     ],
        #     margin=dict(l=50, r=50, t=50, b=50),
        # )
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : Time Spent Percentage Done'))

        return fig_main_list , fig_cotraveler_list


    def get_time_spent(self,table,main_device_id):
        dict_blocks = []
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : Time Spent Under Progress'))
        for index, row in table.iterrows():
            time_spent = row['TOTAL_UNCOMMMON_TIME_SPENT']
            sub_fig_main = self.cotraveler_report_functions.gauge_chart(time_spent,row['DEVICE_ID'],"Uncommon Time Spent Percentage")
            dict_block = {
            'BLOCK_CONTENT': sub_fig_main,
            'BLOCK_NAME': 'Gauge Chart Uncommon Time Spent - {}'.format(row['DEVICE_ID']),
            'BLOCK_TYPE': 'LIST',
            'BLOCK_ASSOCIATION': 'COTRAVELER',
            'DEVICE_ID': main_device_id}
            dict_blocks.append(dict_block)
            
        print(table.head(5),table.columns)
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : Time Spent Done'))
##############################################################################################################################

    def get_cotraveler_kpis(self,table,main_device_id):
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : Cotraveler KPIs Under Progress'))
        kpis_blocks = []
        dict_blocks = []

        for index, row in table.iterrows():
            common_instances = row['COUNT']
            common_instances_similarity = row['GRID_SIMILARITY']*100
            longest_common_sequences = row['LONGEST_SEQUENCE']
            avg_sequence_distance = row['SEQUENCE_DISTANCE']
            classification = row['CLASSIFICATION']
            
            kpis_blocks.append([common_instances,common_instances_similarity,longest_common_sequences,avg_sequence_distance])    
            dict_block = {
            'BLOCK_CONTENT': [classification,common_instances_similarity,longest_common_sequences,avg_sequence_distance],
            'BLOCK_NAME': 'Cotraveler KPIs - {}'.format(row['DEVICE_ID']),
            'BLOCK_TYPE': 'LIST',
            'BLOCK_ASSOCIATION': 'COTRAVELER',
            'DEVICE_ID': main_device_id}
            dict_blocks.append(dict_block)
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : Cotraveler KPIs Done'))
        return kpis_blocks,dict_blocks

##################################################################################################################################
    def get_common_countires(self,table:pd.DataFrame = None):


        fig_list = []
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : Cotraveler Common Countries Map Under Progress'))

        for index, row in table.iterrows():
            country_tuples = row['COUNTRY_TUPLES']
            # common_countries_list = country_tuples[0]
            # comon_cities_list = country_tuples[1]
            # common_lat_long_list = country_tuples[2]
            # number_of_hits_list = country_tuples[3]
            m = folium.Map(location=[0,0], zoom_start=3)
            # if not isinstance(comon_cities_list, list):
            #     print("comon_cities_list is not a list. Skipping the loop.")
            # else:
            for i, list in enumerate(country_tuples, start=0):

                lat_long = list[2]
                coords = ast.literal_eval(lat_long)
                lat, lon = float(coords[0]), float(coords[1])
                common_country = list[0]
                city = list[1]
                number_of_hits = list[3]
                common_country = common_country.lower()
                image = self.properties.passed_filepath_reports_flags + f'{common_country}.png'

                icon = folium.CustomIcon(
                        image,
                        icon_size=(38, 95),
                        icon_anchor=(22, 94)
                    )
                folium.Marker(
                    location=[lat, lon],
                    
                    # popup=f'City : {city}<br> Number Of Hits:{number_of_hits}',
                    popup=f"""<table style=\'width:100%\'>
                                <tr>
                                    <th>Country</th>
                                    <th>City</th>
                                    <th>Number Of Hits</th>
                                </tr>
                                <tr>
                                    <td>{common_country}</td>
                                    <td>{city}</td>
                                    <td>{number_of_hits}</td>
                                </tr>
                                </table>
                                """,
                    icon=icon
                ).add_to(m)
                # m.save(f"my_map{index}.html")

            fig_list.append(m)
        return fig_list
    
    def get_common_countires_timeline(self,table:pd.DataFrame = None):
        fig_list = []
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : Cotraveler Common Countries Map Under Progress'))

        for index, row in table.iterrows():
            country_tuples = row['COUNTRY_TUPLES']
            # country_tuples = ast.literal_eval(country_tuples)
            country_tuples = eval(country_tuples)

            # common_countries_list = country_tuples[0]
            # comon_cities_list = country_tuples[1]
            # common_lat_long_list = country_tuples[2]
            # number_of_hits_list = country_tuples[3]
            m = folium.Map(location=[0,0], zoom_start=3)
            # if not isinstance(comon_cities_list, list):
            #     print("comon_cities_list is not a list. Skipping the loop.")
            # else:
            features = []
            lat_long_list = []
            time_list = []
            for i, list in enumerate(country_tuples, start=0):
                lat_long = list[2]
                coords = ast.literal_eval(lat_long)
                lat, lon = float(coords[0]), float(coords[1])
                lat_long_list.append([lon,lat])
                common_country = list[0]
                city = list[1]
                number_of_hits = list[3]
                time = list[4]
                time_list.append(time)
                common_country = common_country.lower()
                url = f'https://flagicons.lipis.dev/flags/4x3/{common_country}.svg'
                # image = self.properties.passed_filepath_reports_flags + f'{common_country}.png'
                popup=f"""<table style=\'width:100%\'>
                                <tr>
                                    <th>Country</th>
                                    <th>City</th>
                                    <th>NumberOfHits</th>
                                    <th>Time</th>
                                </tr>
                                <tr>
                                    <td>{common_country}</td>
                                    <td>{city}</td>
                                    <td>{number_of_hits}</td>
                                    <td>{time}</td>
                                </tr>
                                </table>
                                """
                # icon = folium.CustomIcon(
                #         image,
                #         icon_size=(38, 95),
                #         icon_anchor=(22, 94)
                #     )
                feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat],
                },
                "properties": {
                    "time": time,
                    "popup": popup,
                    "id": "house",
                    "icon": "marker",
                    "iconstyle": {
                        "iconUrl": url,
                        "iconSize": [40, 40],
                    },
                },
                }
                features.append(feature)

            features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": lat_long_list,
                },
                "properties": {
                    "popup": "Current address",
                    "times": time_list,
                    "icon": "circle",
                    "iconstyle": {
                        "fillColor": "green",
                        "fillOpacity": 0.6,
                        "stroke": "false",
                        "radius": 40,
                    },
                    "style": {"weight": 0},
                    "id": "man",
                },
            })

            # folium.plugins.TimestampedGeoJson(
            # {"type": "FeatureCollection", "features": features},    
            # period="P1D",
            # duration='P1D',
            # add_last_point=True,
            # auto_play=False,
            # loop=False,
            # max_speed=1,
            # loop_button=True,
            # time_slider_drag_update=True,
            # ).add_to(m)
            folium.plugins.TimestampedGeoJson(
                    {
                        "type": "FeatureCollection",
                        "features": features,
                    },
                    period="PT1H",
                    add_last_point=True,
                    time_slider_drag_update=True,
                    auto_play=False,
                    duration="PT2H",
                ).add_to(m)

            fig_list.append(m)
        return fig_list
    

    

    def get_common_areas_plot(self,device_id_main,df_main,df_common,df_history,table):
        fig_list = []
        dict_blocks = []    
        unique_device_ids = table['DEVICE_ID'].unique()
        for device_id in unique_device_ids:
            df_device = df_history[df_history['device_id'] == device_id]
            df_device_common = df_common[df_common['device_id'] == device_id]
            sub_fig_main = self.cotraveler_report_functions.common_area_plot(df_main=df_main,df_device = df_device,df_device_common= df_device_common)
            fig_list.append(sub_fig_main)
            dict_block = {
            'BLOCK_CONTENT': sub_fig_main,
            'BLOCK_NAME': 'Common Areas Plot - {}'.format(device_id),
            'BLOCK_TYPE': 'PLOTLY',
            'BLOCK_ASSOCIATION': 'COTRAVELER',
            'DEVICE_ID': device_id_main}
            dict_blocks.append(dict_block)
    
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting :Common Area Plot Done'))

        return fig_list,dict_blocks
    
    def get_hit_distirbutions_week(self,device_id_main,df_main,df_common,table):
        fig_list = []
        dict_blocks = []
        unique_device_ids = table['DEVICE_ID']
        for device_id in unique_device_ids:
            df_device = df_common[df_common['device_id'] == device_id]
            sub_fig_main = self.cotraveler_report_functions. hit_distirbution_graph(df_main,df_device)
            fig_list.append(sub_fig_main)
            dict_block = {
            'BLOCK_CONTENT': sub_fig_main,
            'BLOCK_NAME': 'Number of Hits Per Week - {}'.format(device_id),
            'BLOCK_TYPE': 'PLOTLY',
            'BLOCK_ASSOCIATION': 'COTRAVELER',
            'DEVICE_ID': device_id_main}
            dict_blocks.append(dict_block)
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : Data Distribution Done'))

        return fig_list,dict_blocks
    
    def get_hit_distirbutions_graph_month(self,device_id_main,df_main,df_common,table):
        fig_list = []
        dict_blocks = []
        unique_device_ids = table['DEVICE_ID']
        for device_id in unique_device_ids:
            df_device = df_common[df_common['device_id'] == device_id]
            sub_fig_main = self.cotraveler_report_functions.hit_distirbution_graph_month(df_main,df_device)
            fig_list.append(sub_fig_main)
            dict_block = {
            'BLOCK_CONTENT': sub_fig_main,
            'BLOCK_NAME': 'Number of Hits Per Month - {}'.format(device_id),
            'BLOCK_TYPE': 'PLOTLY',
            'BLOCK_ASSOCIATION': 'COTRAVELER',
            'DEVICE_ID': device_id_main}
            dict_blocks.append(dict_block)

        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : Data Distribution Done'))

        return fig_list,dict_blocks


