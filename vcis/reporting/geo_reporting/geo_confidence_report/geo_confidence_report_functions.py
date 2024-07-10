##############################################################################################################################
    # Imports
##############################################################################################################################

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from vcis.utils.utils import CDR_Properties
from vcis.utils.utils import CDR_Utils

class GeoConfidenceReportFunctions():
    def __init__(self):
        self.properties = CDR_Properties()
        self.utils = CDR_Utils()
        self.figure_layout = {'paper_bgcolor': 'rgba(255, 255, 255, 1)',
                                'plot_bgcolor': 'rgba(255, 255, 255, 1)'}
        
        self.title_font = dict(family="Bahnschrift SemiBold", size=20, color='#03045E')
        self.axes_font = dict(family="Bahnschrift SemiBold", size=15, color='#03045E')
        
##############################################################################################################################
    # Reporting Functions
##############################################################################################################################       

    def gauge_chart_color_palette(self, color_range, percentage):
        for entry in color_range:
            if percentage >= entry['range'][0] and percentage <= entry['range'][1]:
                return entry['color']

    def confidence_score_gauge_chart(self, confidence_analysis):
        confidence_score = confidence_analysis['CONFIDENCE_SCORE'].values[0]

        color_range = [
            {'range': [0, 10], 'color': '#FF0000'},   # Red
            {'range': [10, 20], 'color': '#FF4500'},  # Red to Orange-Red gradient
            {'range': [20, 30], 'color': '#FF8C00'},  # Orange
            {'range': [30, 40], 'color': '#FFD700'},  # Orange to Yellow gradient
            {'range': [40, 50], 'color': '#DAB600'},  # Yellow
            {'range': [50, 60], 'color': '#ADFF2F'},  # Yellow to Green-Yellow gradient
            {'range': [60, 70], 'color': '#32CD32'},  # Lime Green
            {'range': [70, 80], 'color': '#008000'},  # Green
            {'range': [80, 90], 'color': '#006400'},  # Dark Green
            {'range': [90, 100], 'color': '#000080'}  # Dark Blue
        ]

        # Determine the color based on the value
        font_color = None
        for range_ in color_range:
            if range_['range'][0] <= confidence_score <= range_['range'][1]:
                font_color = range_['color']
                break

        color = self.gauge_chart_color_palette(color_range, confidence_score)

        gauge_chart = go.Figure(go.Indicator(
            mode="gauge+number",
            value=confidence_score,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Confidence Score", 'font': {'size': self.title_font['size'], 'color': self.title_font['color'], 'family': self.title_font['family']}},
            gauge={
                'axis': {'range': [0, 100], 'tickmode': 'linear', 'tick0': 0, 'dtick': 10, 'tickfont': {'size': self.axes_font['size'], 'color': self.axes_font['color'], 'family': self.axes_font['family']}},
                'bar': {'color': color},
                'steps': [{
                  'range': [0, confidence_score],
                  'color': color
              }],
            },
        ))

        # Set the font color based on the determined color
        gauge_chart.update_layout(font={'color': font_color, 'family': self.title_font['family']}, 
                                  paper_bgcolor="white", height=300)
        
        confidence_score_description = f"A comprehensive assessment of the reliability ({round(confidence_score, 1)}) of the geospatial data analysis results which is affected by the other consistency metrics."

        # Create a dictionary for the block
        figure_block = {
            'BLOCK_CONTENT': gauge_chart,
            'BLOCK_NAME': 'Gauge Chart',
            'BLOCK_TYPE': 'PLOTLY',
            'BLOCK_ASSOCIATION': 'AOI',
            'DEVICE_ID': confidence_analysis['DEVICE_ID_GEO'].values[0]
        }

        # Create a dictionary for the figure description
        description_block = {
            'BLOCK_CONTENT': confidence_score_description,
            'BLOCK_NAME': 'Confidence Score Description',
            'BLOCK_TYPE': 'TEXT',
            'BLOCK_ASSOCIATION': 'AOI',
            'DEVICE_ID': confidence_analysis['DEVICE_ID_GEO'].values[0]
        }
        
        return figure_block, description_block
    
    def consistency_metrics_radar_chart(self, confidence_analysis):
        # Get consistency metrics
        time_period_consistency = confidence_analysis['TIME_PERIOD_CONSISTENCY'].values[0]
        day_of_week_consistency = confidence_analysis['DAY_OF_WEEK_CONSISTENCY'].values[0]
        hits_per_day_consistency = confidence_analysis['HITS_PER_DAY_CONSISTENCY'].values[0]
        hour_of_day_consistency = confidence_analysis['HOUR_OF_DAY_CONSISTENCY'].values[0]
        
        # Create dataframe for polar chart
        df = pd.DataFrame(dict(
                    r = [time_period_consistency, hits_per_day_consistency, hour_of_day_consistency, day_of_week_consistency],
                    theta = ['Time Period Consistency','Hits Per Day Consistency',
                             'Hour of Day Consistency','Day of Week Consistency']))
        
        # Create polar chart
        fig = px.line_polar(df, r='r', theta='theta', line_close=True, template='gridon')
        fig.update_traces(line=dict(color='darkblue'), fill='toself', fillcolor='lightblue', opacity = 0.7)
        
        # Customize layout for title and axes font
        fig.update_layout(
            title=dict(
                text='Consistency Metrics',
                font=self.title_font
            ),
            paper_bgcolor="white", 
            height=350,
            polar = dict(
                radialaxis = dict(range=[0, 100], showticklabels=False, tickfont=self.axes_font),
                angularaxis = dict(showticklabels=True, tickfont=self.axes_font)
            )
        )
        
        metrics = [time_period_consistency, day_of_week_consistency, hits_per_day_consistency, hour_of_day_consistency]
        
        # Create a dictionary for the figure block
        figure_block = {
            'BLOCK_CONTENT': fig,
            'BLOCK_NAME': 'Confidence Metrics',
            'BLOCK_TYPE': 'PLOTLY',
            'BLOCK_ASSOCIATION': 'AOI',
            'DEVICE_ID': confidence_analysis['DEVICE_ID_GEO'].values[0]  
        }
        
        confidence_metrics_explanation = f"A time period consistency of {metrics[0]}, day of week consistency of {metrics[1]}, hits per day consistency of {metrics[2]}, and hour of day consistency of {metrics[3]}"

        # Create a dictionary for the figure description        
        description_block = {
            'BLOCK_CONTENT': confidence_metrics_explanation,
            'BLOCK_NAME': 'Confidence Metrics Explanation',
            'BLOCK_TYPE': 'TEXT',
            'BLOCK_ASSOCIATION': 'AOI',
            'DEVICE_ID': confidence_analysis['DEVICE_ID_GEO'].values[0]
        }

        return figure_block, description_block