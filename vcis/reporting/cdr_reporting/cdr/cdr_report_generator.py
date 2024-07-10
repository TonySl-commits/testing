##############################################################################################################################
    # Imports
##############################################################################################################################

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import timedelta
import folium
from folium.plugins import Geocoder , MeasureControl ,MousePosition,SemiCircle,FeatureGroupSubGroup
import math

from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.reporting.report_main.reporting_tools import ReportingTools

class CdrReportGenerator():
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.properties = CDR_Properties()
        self.utils = CDR_Utils(verbose=self.verbose)
        self.cassandra_tools = CassandraTools()
        self.cassandra_spark_tools = CassandraSparkTools()
        self.reporting_tools = ReportingTools(self.verbose)
        self.title_font = dict(family="Bahnschrift SemiBold", size=20, color='#03045E')
        
##############################################################################################################################
    # Reporting Functions
##############################################################################################################################
  
    def get_cgi_count_histogram(self,df):
        print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★{:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : CGI Recoreds Hit Count Under Progress'))
        grouped_df = df.groupby('cgi_id').size().reset_index(name='count')

        top_10 = grouped_df.sort_values(by='count', ascending=False).head(10)

        # Create bar chart using Plotly
        fig = go.Figure(data=[go.Bar(x=top_10['cgi_id'], y=top_10['count'])])
        fig.update_layout(
                title={'text': 'Top 10 CGI_ID','font': {'size': self.title_font['size'], 'color': self.title_font['color'], 'family': self.title_font['family']},
                    'x': 0.5, 
                    'y': 0.9 }, 
                xaxis_title='CGI_ID', yaxis_title='Count',               
                font={'family': self.title_font['family']},
                paper_bgcolor="white",
                height=500)

        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★{:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : Mapping Shared Instances Across Time Under Progress'))
        return fig
    
##############################################################################################################################

    def get_map_cgi_usage_folium(self, df , radius:int = 100):
        print(df.head())
        df.to_csv('cgi_usage.csv')
        grouped_df = df.groupby(['cgi_id','location_azimuth','location_latitude','location_longitude']).size().reset_index(name='count')
        print(grouped_df)
        bts_table= self.cassandra_tools.get_bts_table(server = '10.1.10.110')
        
        bts_table = bts_table[['cgi_id','network_type_code','bts_cell_name','site_name','uafrcn_code']]
        
        grouped_df = pd.merge(grouped_df, bts_table, on='cgi_id', how='left')

        m = folium.Map(location=[grouped_df['location_latitude'].mean(),grouped_df['location_longitude'].mean()], zoom_start=15)
        folium.plugins.Geocoder().add_to(m)
        fg = folium.FeatureGroup(name="Technology")
        m.add_child(fg)
        
        g1 = folium.plugins.FeatureGroupSubGroup(fg, "2G")
        m.add_child(g1)

        g2 = folium.plugins.FeatureGroupSubGroup(fg, "3G")
        m.add_child(g2)
        
        g3 = folium.plugins.FeatureGroupSubGroup(fg, "4G")
        m.add_child(g3)
        
        
        for index, row in grouped_df.iterrows():
            # Calculate start and stop angles
            azimuth_degrees = int(row['location_azimuth'])
            # print(type(azimuth_degrees))
            start_angle = (azimuth_degrees) % 360  # Offset by 60 degrees
            stop_angle = (azimuth_degrees + 60) % 360   # Offset by 60 degrees

            if row['network_type_code'] == '1':
                folium.plugins.SemiCircle(
                    (row['location_latitude'], row['location_longitude']),
                    radius=radius,
                    start_angle=start_angle,
                    stop_angle=stop_angle,
                    color="red",
                    fill_color="red",
                    opacity=0.5,
                    popup=f"CGI_ID: {row['cgi_id']}<br>Technology: 2G<br>BTS Site Name:{row['site_name']}<br>BTS Frequancy:{row['uafrcn_code']}<br>BTS Cell Name:{row['bts_cell_name']}<br>Count: {row['count']}",
                ).add_to(g1)
            elif row['network_type_code'] == '2':
                folium.plugins.SemiCircle(
                    (row['location_latitude'], row['location_longitude']),
                    radius=radius,
                    start_angle=start_angle,
                    stop_angle=stop_angle,
                    color="blue",
                    fill_color="blue",
                    opacity=0.5,
                    popup=f"CGI_ID: {row['cgi_id']}<br>Technology: 3G<br>BTS Site Name:{row['site_name']}<br>BTS Frequancy:{row['uafrcn_code']}<br>BTS Cell Name:{row['bts_cell_name']}<br>Count: {row['count']}",
                ).add_to(g2)
            elif row['network_type_code'] == '3':
                folium.plugins.SemiCircle(
                    (row['location_latitude'], row['location_longitude']),
                    radius=radius,
                    start_angle=start_angle,
                    stop_angle=stop_angle,
                    color="green",
                    fill_color="green",
                    opacity=0.5,
                    popup=f"CGI_ID: {row['cgi_id']}<br>Technology: 4G<br>BTS Site Name:{row['site_name']}<br>BTS Frequancy:{row['uafrcn_code']}<br>BTS Cell Name:{row['bts_cell_name']}<br>Count: {row['count']}",
                ).add_to(g3)
        folium.LayerControl(collapsed=True).add_to(m)
        m.add_child(folium.LatLngPopup())
        m.add_child(MeasureControl())
        MousePosition().add_to(m)
        if self.verbose:
            print('★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★{:<24} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ '.format('Reporting : Mapping Cell Tower Usage Under Progress')) 
        return m , bts_table