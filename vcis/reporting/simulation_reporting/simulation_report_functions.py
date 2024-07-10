##############################################################################################################################
    # Imports
##############################################################################################################################

import plotly.express as px
import plotly
import plotly.graph_objs as go
import pandas as pd
import calendar

from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.utils.utils import CDR_Properties
from vcis.utils.utils import CDR_Utils
class SimulationReportFunctions():
    def __init__(self):
        self.cassandra_tools = CassandraTools()
        self.properties = CDR_Properties()
        self.utils = CDR_Utils()
        self.dow_color_palette = ['#03045E', '#023E8A', '#0077B6', '#0096C7', '#00B4D8', '#48CAE4', '#90E0EF', '#ADE8F4', '#CAF0F8']
        self.months_color_palette = ['#1130A7', '#125F92', '#128F7E', '#13BE69', '#13EE55', '#A4D82F', '#ECCD1C', '#FC9C14', '#CB620F', '#9A270A', '#561463', '#1100BB']
        self.figure_layout = {'paper_bgcolor': 'rgba(255, 255, 255, 1)',
                                'plot_bgcolor': 'rgba(255, 255, 255, 1)'}
        self.title_font = dict(family="Bahnschrift SemiBold", size=20, color='#03045E')
        self.axes_font = dict(family="Bahnschrift SemiBold", size=15, color='#03045E')
##############################################################################################################################
    # Reporting Functions
##############################################################################################################################       
    
    def read_data(self, filepath,report_type):
        data = pd.read_csv(filepath)
        if report_type==11:
            data = data.rename(columns={'USAGE_TIMEFRAME':'Timestamp'})

        else:
            try:
                data.drop(columns=['CALLING_NO', 'CALLED_NO', 'LOCATION_ACCURACY', 'CALL_EDATE', 'CALL_BDATE', 'DEVICE_CARRIER_NAME', 'USAGE_DATE', 'LOCATION_MAIN_DATA_ID', 'LOCATION_COU_CODE_NUM'], inplace=True)
            except:
                data.drop(columns=[ 'LOCATION_ACCURACY', 'DEVICE_CARRIER_NAME', 'USAGE_DATE', 'LOCATION_MAIN_DATA_ID', 'LOCATION_COU_CODE_NUM'], inplace=True)

        data = data.rename(columns={'USAGE_TIMEFRAME':'Timestamp'})
        
        data['Timestamp'] = pd.to_datetime(data['Timestamp'], unit='ms')
        data['Date'] = data['Timestamp'].dt.date
        data['Month'] = data['Timestamp'].dt.to_period('M')
        data['DayOfWeek'] = data['Timestamp'].dt.day_name()
        data['HourOfDay'] = data['Timestamp'].dt.hour

        return data
    
    def number_of_hits_per_dow(self, data):
        # Count hits per day of the week
        hits_per_dow = data['DayOfWeek'].value_counts().reset_index(name='NumberOfHits')
        hits_per_dow.columns = ['DayOfWeek', 'NumberOfHits']

        # Ensure all days of the week are present, even if no hits
        all_days = pd.DataFrame({'DayOfWeek': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']})
        hits_per_dow = pd.merge(all_days, hits_per_dow, how='left', on='DayOfWeek').fillna(0)

        print('INFO:    HITS PER DOW\n', hits_per_dow)

        # Create bar plot with text labels inside the bars
        fig = px.bar(hits_per_dow, x='DayOfWeek', y='NumberOfHits', title='Number of Hits per Day of Week',
                    labels={'DayOfWeek': 'Day of Week', 'NumberOfHits': 'Number of Hits'},
                    text='NumberOfHits')  # Use 'NumberOfHits' column for text labels

        # Layout for title and axes font
        fig.update_layout(
            title={
                'text': 'Number of Hits per Day of Week',
                'font': self.title_font
            },
            xaxis_title_text='Day of Week',  # X-axis title
            yaxis_title_text='Number of Hits',  # Y-axis title
            xaxis=dict(
                titlefont=self.axes_font,
                tickfont=self.axes_font
            ),
            yaxis=dict(
                titlefont=self.axes_font,
                tickfont=self.axes_font
            )
        )

        fig.update_layout(self.figure_layout)

        fig.update_traces(marker_color=self.dow_color_palette, textposition='inside', textfont=dict(color='white'))  # Set text position inside the bars and white color for text
        return fig

    def number_of_hits_per_month(self, data):
        # Aggregate data by Month and count the number of hits
        hits_per_month = data.groupby('Month').size().reset_index(name='NumberOfHits')

        # Create a DataFrame for all months in the year
        year = data['Timestamp'].dt.year.iloc[0]  # Use the year from the first record
        all_months = [f"{year}-{month:02d}" for month in range(1, 13)]
        all_months_df = pd.DataFrame({'Month': pd.PeriodIndex(all_months, freq='M')})

        # Merge the aggregated data with the all_months_df DataFrame
        hits_per_month = pd.merge(all_months_df, hits_per_month, on='Month', how='left').fillna(0)

        # Convert 'Month' to timestamps to extract the month names
        hits_per_month['Month'] = hits_per_month['Month'].dt.start_time
        hits_per_month['MonthName'] = hits_per_month['Month'].dt.month.apply(lambda x: calendar.month_name[x])

        print('INFO:    HITS PER MONTH\n', hits_per_month)

        # Create bar plot with text labels inside the bars
        fig = px.bar(hits_per_month, x='MonthName', y='NumberOfHits', title='Number of Hits per Month',
                    color='MonthName', color_discrete_sequence=self.months_color_palette,
                    text='NumberOfHits')  # Use 'NumberOfHits' column for text labels

        # Layout for title and axes font
        fig.update_layout(
            title={
                'text': 'Number of Hits per Month',
                'font': self.title_font
            },
            xaxis_title_text='Months',  # X-axis title
            yaxis_title_text='Number of Hits',  # Y-axis title
            xaxis=dict(
                titlefont=self.axes_font,
                tickfont=self.axes_font
            ),
            yaxis=dict(
                titlefont=self.axes_font,
                tickfont=self.axes_font
            )
        )

        fig.update_layout(self.figure_layout)

        fig.update_traces(textposition='inside', textfont=dict(color='white'))  # Set text position inside the bars and white color for text

        return fig
    
    def number_of_hits_per_hod(self, data):
        # Compute the number of hits per Hour of Day
        hits_per_hod = data.groupby('HourOfDay').size().reset_index(name='NumberOfHits')
        hits_per_hod = hits_per_hod.reindex(range(0, 24), fill_value=0)

        print('INFO:    HITS PER HOUR OF DAY\n', hits_per_hod)

        # Create bar plot with text labels inside the bars
        fig = px.bar(hits_per_hod, x='HourOfDay', y='NumberOfHits', title='Number of Hits per Hour of Day',
                    labels={'HourOfDay': 'Hour of Day', 'NumberOfHits': 'Number of Hits'},
                    text='NumberOfHits')  # Use 'NumberOfHits' column for text labels

        # Layout for title and axes font
        fig.update_layout(
            title={
                'text': 'Number of Hits per Hour of Day',
                'font': self.title_font
            },
            xaxis_title_text='Hour of Day',  # X-axis title
            yaxis_title_text='Number of Hits',  # Y-axis title
            xaxis=dict(
                titlefont=self.axes_font,
                tickfont=self.axes_font
            ),
            yaxis=dict(
                titlefont=self.axes_font,
                tickfont=self.axes_font
            )
        )

        fig.update_layout(self.figure_layout)

        fig.update_traces(marker_color=self.dow_color_palette, textposition='inside', textfont=dict(color='white'))  # Set text position inside the bars and white color for text

        return fig

    def number_of_hits_per_hod(self, data):
        # Compute the number of hits per Hour of Day
        hits_per_hod = data.groupby('HourOfDay').size().reset_index(name='NumberOfHits')
        hits_per_hod = hits_per_hod.reindex(range(0, 24), fill_value=0)

        print('INFO:    HITS PER HOUR OF DAY\n', hits_per_hod)

        # Create line plot with text labels on markers
        fig = go.Figure()

        fig.add_trace(go.Scatter(x=hits_per_hod['HourOfDay'], y=hits_per_hod['NumberOfHits'],
                                mode='lines+markers', name='Number of Hits per Hour of Day',
                                line=dict(color='darkblue', width=3),
                                marker=dict(color='darkblue', size=10),
                                text=hits_per_hod['NumberOfHits'], textposition='top center'))

        # Layout for title and axes font
        fig.update_layout(
            title={
                'text': 'Number of Hits per Hour of Day',
                'font': self.title_font
            },
            xaxis_title_text='Hour of Day',  # X-axis title
            yaxis_title_text='Number of Hits',  # Y-axis title
            xaxis=dict(
                titlefont=self.axes_font,
                tickfont=self.axes_font
            ),
            yaxis=dict(
                titlefont=self.axes_font,
                tickfont=self.axes_font
            )
        )

        fig.update_layout(self.figure_layout)

        return fig

    
    def summary_statistics(self, data):
        # Group by device id and calculate the number of hits and days recorded for each device
        summary_stats = data.groupby('DEVICE_ID').agg(Number_of_Hits=('Timestamp', 'count'),
                                                    Number_of_Days=('Date', 'nunique')).reset_index()

        print('INFO:    SUMMARY STATISTICS\n', summary_stats)

        return summary_stats
    
    def process_data(self, data, report_type):
        if report_type==11:
            data = data.rename(columns={'USAGE_TIMEFRAME':'Timestamp'})

        else:
            data.drop(columns=['CALLING_NO', 'CALLED_NO', 'LOCATION_ACCURACY', 'CALL_EDATE', 'CALL_BDATE', 'USAGE_DATE', 'LOCATION_MAIN_DATA_ID', 'LOCATION_COU_CODE_NUM'], inplace=True)
            data = data.rename(columns={'USAGE_TIMEFRAME':'Timestamp'})

        
        data['Timestamp'] = pd.to_datetime(data['Timestamp'], unit='ms')
        data['Date'] = data['Timestamp'].dt.date
        data['Month'] = data['Timestamp'].dt.to_period('M')
        data['DayOfWeek'] = data['Timestamp'].dt.day_name()
        data['HourOfDay'] = data['Timestamp'].dt.hour

        return data
    
    ###########################################################################################################
    # GeoSpatial Data Quality Comparison wrt Service Provider ID
    ###########################################################################################################

    def hits_per_service_provider_id(self, data):
        hits_per_service_provider_id = data.groupby('SERVICE_PROVIDER_ID').size().reset_index(name='NumberOfHits')
        print(hits_per_service_provider_id)
        
        # Assuming hits_per_server_provider is your DataFrame
        labels = hits_per_service_provider_id['SERVICE_PROVIDER_ID']
        values = hits_per_service_provider_id['NumberOfHits']

        # Calculate the total number of hits
        total_hits = values.sum()

        # Calculate the percentage for each server provider
        percentages = (values / total_hits) * 100

        # Create a list of strings for the hover text, including server provider name, number of hits, and percentage
        hover_text = [f"Server Provider: {label}<br>Number Of Hits: {value}<br>Percentage:{percentage:.2f}%" for label, value, percentage in zip(labels, values, percentages)]

        # Create a donut chart
        fig = go.Figure(data=[go.Pie(labels=labels, 
                                    values=values, 
                                    hole=0.6,
                                    hovertext=hover_text, 
                                    hoverinfo='text',
                                    marker_colors=self.dow_color_palette)])

        # Layout for title and axes font
        fig.update_layout(
            title={
                'text': 'Percentage of Hits per Service Provider',
                'font': self.title_font
            }
        )

        fig.update_layout(self.figure_layout)

        return fig 
    
    def consistency_metrics_grouped_bar_chart(self, data):

        required_columns = ['SERVICE_PROVIDER_ID', 'TIME_PERIOD_CONSISTENCY', 'HITS_PER_DAY_CONSISTENCY', 'HOUR_OF_DAY_CONSISTENCY', 'DAY_OF_WEEK_CONSISTENCY', 'CONFIDENCE_SCORE']
        missing_columns = [col for col in required_columns if col not in data.columns]

        if missing_columns:
            raise KeyError(f'The following required columns are missing from the dataset: {missing_columns}')
        
        # Rename the columns
        data = data.rename(columns={'TIME_PERIOD_CONSISTENCY': 'Time Period Consistency',
                                    'HITS_PER_DAY_CONSISTENCY': 'Hits Per Day Consistency',
                                    'HOUR_OF_DAY_CONSISTENCY': 'Hour of Day Consistency',
                                    'DAY_OF_WEEK_CONSISTENCY': 'Day of Week Consistency',
                                    'CONFIDENCE_SCORE': 'Confidence Score',
                                    'SERVICE_PROVIDER_ID': 'Service Provider ID'})
        
        # Melt the dataframe to have a suitable format for the plot
        df_melted = data.melt(id_vars=['Service Provider ID'],
                              value_vars=['Time Period Consistency', 'Hits Per Day Consistency', 'Hour of Day Consistency', 'Day of Week Consistency', 'Confidence Score'],
                              var_name='METRIC', value_name='VALUE')
        
        # Ensure that values are capped at 100%
        df_melted['VALUE'] = df_melted['VALUE'].clip(upper=100)

        # Create histogram
        fig = px.histogram(df_melted, x='METRIC', y='VALUE', color='Service Provider ID',
                           barmode='group', # grouped bars for each service provider
                           color_discrete_sequence=self.dow_color_palette)
        
        # Layout for title and axes font
        fig.update_layout(
            title={
                'text': 'Consistency Metrics & Confidence Score per Service Provider',
                'font': self.title_font
            },
            xaxis_title_text='Consistency Metrics & Confidence Score',  # X-axis title
            yaxis_title_text='Percentage',  # Y-axis title
            xaxis=dict(
                titlefont=self.axes_font,
                tickfont=self.axes_font
            ),
            yaxis=dict(
                titlefont=self.axes_font,
                tickfont=self.axes_font,
                range=[0, 100],  # Set y-axis range from 0 to 100
                tickmode='linear',  # Use linear ticks
                dtick=10,  # Set tick step
                gridcolor='rgba(0, 0, 0, 0.1)',  # Grid color
                gridwidth=1  # Grid width
            )
        )

        fig.update_layout(self.figure_layout)

        return fig


    
