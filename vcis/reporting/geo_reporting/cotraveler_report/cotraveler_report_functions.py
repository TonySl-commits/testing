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
import plotly.express as px

from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils

class CotravelerReportFunctions():
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.properties = CDR_Properties()
        self.utils = CDR_Utils(verbose=self.verbose)
        self.title_font = dict(family="Bahnschrift SemiBold", size=20, color='#03045E')
        self.axes_font = dict(family="Bahnschrift SemiBold", size=15, color='#03045E')
        
##############################################################################################################################
    # Reporting Functions
##############################################################################################################################       
    def calculate_time_spent(self,df,grid_coulmn='grid_x'):
            df['usage_timeframe'] = pd.to_datetime(df['usage_timeframe'])
            
            # Sort the DataFrame by 'usage_timeframe'
            df = df.sort_values(by='usage_timeframe')
            
            # Initialize variables
            total_time_spent = timedelta()
            current_device = None
            current_device_start_time = None
            
            # Iterate through rows
            for index, row in df.iterrows():
                devices = row[grid_coulmn].split(',')
                
                if current_device is None:
                    current_device = devices
                    current_device_start_time = row['usage_timeframe']
                elif devices == current_device:
                    time_difference = row['usage_timeframe'] - current_device_start_time
                    if time_difference < pd.Timedelta(hours=1):
                        total_time_spent += time_difference
                    # else:
                    #     total_time_spent += 0
                    current_device_start_time = row['usage_timeframe']
                else:
                    current_device = devices
                    current_device_start_time = row['usage_timeframe']
            
            return round(total_time_spent.total_seconds() / 60)
        
    def get_color_for_percentage(self, color_range, percentage):
        for entry in color_range:
            if percentage >= entry['range'][0] and percentage <= entry['range'][1]:
                return entry['color']

    def gauge_chart (self,value,device_id,title):
        confidence_score = value

        color_range = [
            {'range': [0, 10], 'color': '#FF0000'},   # Red
            {'range': [10, 20], 'color': '#FF4500'},  # Red to Orange-Red gradient
            {'range': [20, 30], 'color': '#FF8C00'},  # Orange
            {'range': [30, 40], 'color': '#FFD700'},  # Orange to Yellow gradient
            {'range': [40, 50], 'color': '#FFFF00'},  # Yellow
            {'range': [50, 60], 'color': '#ADFF2F'},  # Yellow to Green-Yellow gradient
            {'range': [60, 70], 'color': '#32CD32'},  # Lime Green
            {'range': [70, 80], 'color': '#008000'},  # Green
            {'range': [80, 90], 'color': '#006400'},  # Dark Green
            {'range': [90, 100], 'color': '#000080'}  # Dark Blue
        ]

        # Determine the color based on the value
        font_color = None
        for range_ in color_range:
            if range_['range'][0] <= confidence_score * 100 <= range_['range'][1]:
                font_color = range_['color']
                break

        color = self.get_color_for_percentage(color_range, confidence_score * 100)

        gauge_chart =go.Indicator(
            mode="gauge+number",
            value=confidence_score * 100,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': title , 'font': {'size': self.title_font['size'], 'color': self.title_font['color'], 'family': self.title_font['family']}},
            gauge={
                'axis': {'range': [0, 100], 'tickmode': 'linear', 'tick0': 0, 'dtick': 10, 'tickfont': {'size': self.axes_font['size'], 'color': self.axes_font['color'], 'family': self.axes_font['family']}},
                'bar': {'color': color},
                'steps': [{
                  'range': [0, confidence_score * 100],
                  'color': color
              }],
            },
            number={
                'valueformat': '.0f',  
                'suffix': '%',       
            },
            name=f'device_id {device_id}'
        )
        gauge_chart = go.Figure(data=gauge_chart)
        gauge_chart.update_layout(font={'color': font_color, 'family': self.title_font['family']}, 
                                  paper_bgcolor="white", height=300)
        return gauge_chart 


    def common_area_plot(self,df_main,df_device,df_device_common):

        fig = go.Figure()

        fig.add_trace(go.Scattermapbox(
            lat=df_main['location_latitude'],
            lon=df_main['location_longitude'],
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=6,
                color='#ed5151',
                opacity=0.8
            ),
            text=df_main['device_id'], 
            name='Main Device'
        ))

        # Add a trace for df_device
        fig.add_trace(go.Scattermapbox(
            lat=df_device['location_latitude'],
            lon=df_device['location_longitude'],
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=6,
                color='#149ece',
                opacity=0.8
            ),
            text="Cotraveler Device", 
            name=f'Cotraveler Device'
        ))

        for index, row in df_device_common.iterrows():
            lat, lon = map(float, row['grid'].split(','))
            lat_min, lon_min, lat_max, lon_max = self.utils.calculate_box_boundaries(lat, lon, 50)
            
            # Draw a box around the point
            fig.add_trace(go.Scattermapbox(
                lat=[lat_min, lat_max, lat_max, lat_min, lat_min],
                lon=[lon_min, lon_min, lon_max, lon_max, lon_min],
                mode='lines',
                line=dict(width=7, color='black'),
                showlegend=False
            ))

        # Set the map style
        fig.update_layout(
            autosize=True,
            hovermode='closest',
            mapbox=dict(
                style='open-street-map',
                bearing=0,
                center=dict(
                    lat=df_main['location_latitude'].mean(), 
                    lon=df_main['location_longitude'].mean()
                ),
                pitch=0,
                zoom=6
            ),
        )
        
        fig.update_layout(
        width=800, 
        height=600,
        )

        return fig


    def hit_distirbution_graph(self,df_main,df_device):
        def hit_distirbution(df):
            df['day'] = pd.to_datetime(df['usage_timeframe']).dt.day_name()
            hits_per_day = df['day'].value_counts().reset_index()
            hits_per_day.columns = ['day', 'nb_hits']
            order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            all_days = pd.DataFrame({'day': order})
            hits_per_day = pd.merge(all_days, hits_per_day, how='left', on='day').fillna(0)
            return hits_per_day
        
        df_main = df_main.drop_duplicates()
        df_device = df_device.drop_duplicates()

        hits_per_day_main = hit_distirbution(df_main)
        hits_per_day_cotraveler = hit_distirbution(df_device)

        trace_main = go.Bar(x=hits_per_day_main['day'], y=hits_per_day_main['nb_hits'], name='Main Data', marker_color='#03045E')
        trace_cotraveler = go.Bar(x=hits_per_day_cotraveler['day'], y=hits_per_day_cotraveler['nb_hits'], name='Co-Traveler Data', marker_color='#0844bf')

        fig = go.Figure([trace_main, trace_cotraveler])

        fig.update_layout(
                title={
                    'text': 'Distribution of Colocated Hits',
                    'font': dict(family="Bahnschrift SemiBold", size=20, color='#03045E')
                },
                xaxis_title_text='Day of Week',  
                yaxis_title_text='Number of Hits',  
                xaxis=dict(
                    titlefont= dict(family="Bahnschrift SemiBold", size=15, color='#03045E'),
                    tickfont= dict(family="Bahnschrift SemiBold", size=15, color='#03045E')
                ),
                yaxis=dict(
                    titlefont= dict(family="Bahnschrift SemiBold", size=15, color='#03045E'),
                    tickfont= dict(family="Bahnschrift SemiBold", size=15, color='#03045E')
                )
        )

        fig.update_layout({'paper_bgcolor': 'rgba(255, 255, 255, 1)',
                                            'plot_bgcolor': 'rgba(255, 255, 255, 1)'})
        return fig
    
    def hit_distirbution_graph_month(self,df_main,df_device):
        
        def hit_distirbution(df):
            df['month'] = pd.to_datetime(df['usage_timeframe']).dt.month_name()
            hits_per_month = df['month'].value_counts().reset_index()
            hits_per_month.columns = ['month', 'nb_hits']
            order = ['January', 'February', 'March', 'April', 'May', 'June', 'July','August','September','October','November', 'December']
            all_month = pd.DataFrame({'month': order})
            hits_per_month = pd.merge(all_month, hits_per_month, how='left', on='month').fillna(0)
            return hits_per_month

        df_main = df_main.drop_duplicates()
        df_device = df_device.drop_duplicates()

        hits_per_month_main = hit_distirbution(df_main)
        hits_per_month_cotraveler = hit_distirbution(df_device)

        trace_main = go.Bar(x=hits_per_month_main['month'], y=hits_per_month_main['nb_hits'], name='Main Data', marker_color='#03045E')
        trace_cotraveler = go.Bar(x=hits_per_month_cotraveler['month'], y=hits_per_month_cotraveler['nb_hits'], name='Co-Traveler Data', marker_color='#0844bf')

        fig = go.Figure([trace_main, trace_cotraveler])

        fig.update_layout(
                title={
                    'text': 'Distribution of Colocated Hits',
                    'font': dict(family="Bahnschrift SemiBold", size=20, color='#03045E')
                },
                xaxis_title_text='Day of Week',  
                yaxis_title_text='Number of Hits',  
                xaxis=dict(
                    titlefont= dict(family="Bahnschrift SemiBold", size=15, color='#03045E'),
                    tickfont= dict(family="Bahnschrift SemiBold", size=15, color='#03045E')
                ),
                yaxis=dict(
                    titlefont= dict(family="Bahnschrift SemiBold", size=15, color='#03045E'),
                    tickfont= dict(family="Bahnschrift SemiBold", size=15, color='#03045E')
                )
        )

        fig.update_layout({'paper_bgcolor': 'rgba(255, 255, 255, 1)',
                                            'plot_bgcolor': 'rgba(255, 255, 255, 1)'})


        return fig
    