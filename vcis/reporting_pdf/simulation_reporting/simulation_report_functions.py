##############################################################################################################################
    # Imports
##############################################################################################################################

import plotly.express as px
import matplotlib.pyplot as plt
import plotly.graph_objs as go
import pandas as pd
import calendar
import country_converter as coco
from reportlab.platypus import Table
from reportlab.lib.units import inch
import folium
import random

from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.utils.utils import CDR_Properties
from vcis.utils.utils import CDR_Utils

class SimulationReportFunctions():
    def __init__(self):
        self.cassandra_tools = CassandraTools()
        self.properties = CDR_Properties()
        self.utils = CDR_Utils()
        self.oracle_tools = OracleTools()
        self.dow_color_palette = ['#03045E', '#023E8A', '#0077B6', '#0096C7', '#00B4D8', '#48CAE4', '#90E0EF', '#ADE8F4', '#CAF0F8']
        self.months_color_palette = ['#1130A7', '#125F92', '#128F7E', '#13BE69', '#13EE55', '#A4D82F', '#ECCD1C', '#FC9C14', '#CB620F', '#9A270A', '#561463', '#1100BB']
        self.figure_layout = {'paper_bgcolor': 'rgba(255, 255, 255, 1)',
                                'plot_bgcolor': 'rgba(255, 255, 255, 1)'}
        self.title_font = dict(family="Bahnschrift SemiBold", size=20, color='#03045E')
        self.axes_font = dict(family="Bahnschrift SemiBold", size=15, color='#03045E')
        self.report_type = ""
        self.table_id = ""
        self.report_name = ""
        
##############################################################################################################################
    # Reporting Functions
##############################################################################################################################       
    
    def get_simulation_data(self,table_id:int=None , table:pd.DataFrame=None):
        self.report_type = table['LOC_REPORT_TYPE'][0]
        self.table_id = table['LOC_REPORT_CONFIG_ID'][0]
        self.report_name = table['LOC_REPORT_NAME'][0]

        if self.report_type==11:
            table = self.oracle_tools.get_table("SSDX_ENG","DATACROWD_CDR_DATA",table_id,True)
        else:    
            table = self.oracle_tools.get_table("SSDX_ENG","TMP_REPORT_COORDINATES_6",table_id,True)

        table = table[[ 'LOCATION_LATITUDE', 'LOCATION_LONGITUDE', 'DEVICE_ID', 'USAGE_TIMEFRAME','LOCATION_NAME','SERVICE_PROVIDER_ID' ]].drop_duplicates()
        table['SERVICE_PROVIDER_ID'] = table['SERVICE_PROVIDER_ID'].apply(lambda x: f"GeoSpatial Provider {x}")
        # table = self.utils.convert(table,date_column_name = 'USAGE_TIMEFRAME')
        table['USAGE_TIMEFRAME'] = pd.to_datetime(table['USAGE_TIMEFRAME'], unit='ms')
        table.columns = table.columns.str.lower()
        table = self.utils.convert_ms_to_datetime(table)
        self.date_only = table['usage_timeframe'].dt.date
        table = self.utils.add_reverseGeocode_columns(table)
        original_table = table.copy()
        table, mapping_table = self.utils.get_simple_ids(table=table)
        table['country_full'] = coco.convert(names=table['country'], to='name_short')
        self.num_devices = len(table['device_id'].unique())
        self.num_days = len(self.date_only.unique())
        self.num_records = len(table)
        self.countries =  table['country_full'].unique()
        self.cities =  table['city'].unique()

        return original_table, table , mapping_table
    
    def get_summary_table(self,table:pd.DataFrame=None):
        messages = f"""
        {self.report_name} Simulation Quick Summary:

        - Number of Devices: {self.num_devices}
        - Number of Records: {self.num_records}
        - Number of Days: {self.num_days}
        - Countries: {', '.join(self.countries)}
        - Cities: {', '.join(self.cities)}
        """
        if len(self.cities) >=4: 
            data = [
                    ['Number of Devices', self.num_devices],
                    ['Number of Records', self.num_records],
                    ['Number of Days', self.num_days],
                    ['Countries', ', '.join(self.countries)],
                    ['Cities Number', len(self.cities)]
            ]
        else:
            data = [
                ['Number of Devices', self.num_devices],
                ['Number of Records', self.num_records],
                ['Number of Days', self.num_days],
                ['Countries', ', '.join(self.countries)],
                ['Cities', ', '.join(self.cities)]
            ]
                # Convert the list of lists into a list of dictionaries
        data_dicts = [{'Statistic': row[0], 'Data': row[1]} for row in data]

        # Create the DataFrame
        summary_table = pd.DataFrame(data_dicts)
        # col_widths = [3*inch, 3*inch]
        # summary_table = Table(data, colWidths=col_widths,repeatRows=1)
        return summary_table, messages
    
    def get_report_introduction (self,table:pd.DataFrame=None,start_date:str='Not Available', end_date:str='Not Available'):
        # try:
        #     start_date = table.loc[0, 'STATUS_BDATE'].date() if table.loc[0, 'STATUS_BDATE'].date() is not None else 'Not Available'
        # except:
        #     start_date = 'Not Available' 
        # try:
        #     end_date = table.loc[0, 'FILTER_EDATE'].date()
        # except:
        #     end_date = 'Not Available'
        if self.report_type == 2:
            report_introduction = self.properties.device_history_pdf_template.format(report_saved_name=self.report_name, start_date=start_date, end_date=end_date, device_or_number=self.num_devices,city_name=', '.join(self.countries))
        if self.report_type == 1:
            report_introduction =  self.properties.activity_scan_pdf_template.format(report_saved_name=self.report_name, start_date=start_date, end_date=end_date,device_or_number=self.num_devices,city_name=', '.join(self.countries))
        if self.report_type==6:
            report_introduction =  self.properties.DHP_template.format(report_saved_name=self.report_name, start_date=start_date, end_date=end_date,device_or_number=self.num_devices)
        print(report_introduction)
        return report_introduction , start_date, end_date
    

    def get_devices_multi_country_city_movement(self,table:pd.DataFrame=None):
        print(self.countries)
        if len(self.countries)> 1:
        # Convert 'usage_timeframe' to datetime
            table['usage_timeframe'] = pd.to_datetime(table['usage_timeframe'])

            # Identify visits based on changes in 'country_full'
            table['visit_number'] = table.groupby('device_id')['country_full'].apply(lambda x: (x != x.shift()).cumsum())
            # df.to_csv('test.csv')
            # print(df)
            # Group by 'device_id', 'country_full', and 'visit_id'
            grouped = table.groupby(['device_id', 'country_full', 'visit_number'])

            # Calculate start and end time for each group
            start_end = grouped.agg(start_time=('usage_timeframe', 'min'), end_time=('usage_timeframe', 'max'))

            # Calculate duration
            start_end['duration'] = start_end['end_time'] - start_end['start_time']

            # Reset index to make 'device_id', 'country_full', and 'visit_number' as columns
            start_end.reset_index(inplace=True)

            # print(start_end)

            # Convert 'start_time' and 'end_time' to datetime
            start_end['start_time'] = pd.to_datetime(start_end['start_time'])
            start_end['end_time'] = pd.to_datetime(start_end['end_time'])
            def assign_threat_score(row):
                if 'Syria' in row['country_full']:
                    return 80
                else:
                    return 0
            start_end['threat_score'] = start_end.apply(assign_threat_score, axis=1)
            threat_score_table = start_end.groupby('device_id')['threat_score'].mean().reset_index()
            threat_score_table = pd.merge(start_end, threat_score_table, on='device_id', suffixes=('', '_y'))

            # Update the 'threat_score' in the first DataFrame with the values from the second DataFrame
            threat_score_table['threat_score'] = threat_score_table['threat_score_y']
            threat_score_table.drop(columns=['threat_score_y'], inplace=True)
            threat_score_table = threat_score_table[threat_score_table['country_full']=='Syria']
            print(threat_score_table)
            threat_score_description=''
            for index, row in threat_score_table.iterrows():                
                # Format the information into a string
                description = f"{row['device_id']} has Threat Score of {row['device_id']} for visiting {row['country_full']} from {row['start_time']} till {row['end_time']} spending {row['duration']} accross {row['visit_number']} visits"
                threat_score_description+=description

            # Create Gantt chart
            fig = px.timeline(start_end, x_start='start_time', x_end='end_time', y='device_id', color='country_full',
                            title='Device Stays in Each Country',
                            labels={'start_time': 'Start Time', 'end_time': 'End Time', 'country_full': 'Country'})

            fig.update_yaxes(categoryorder='total ascending')  # Ensure correct ordering of devices
            fig.update_layout(self.figure_layout,
            title=dict(
                text='Duration at Location per Day of Week',
                font=dict(family="Bahnschrift SemiBold", size=20, color='#002147'),
                x=0.5, # Center the title horizontally
                xanchor='center' # Align the title's center with the x position
            ))
            fig.write_image(self.properties.passed_filepath_reports_png + "plot.png", format='png', width=1000, height=500, scale=1, engine='kaleido')

            movement_text_all_devices = ""
            for device_id in start_end['device_id'].unique():
                device_df = start_end[start_end['device_id'] == device_id].reset_index(drop=True)
                
                movement_text = f"Device {device_id} moved between countries: "
                if len(device_df) >=10:
                    num_devices = 10
                else:
                    num_devices = len(device_df)
                # Iterate over rows to generate movement details
                for i in range(num_devices):
                    if i == 0:
                        movement_text += f"{device_df.loc[i, 'country_full']} from {device_df.loc[i, 'start_time']} to {device_df.loc[i, 'end_time']} and spent {device_df.loc[i, 'duration']} hours"
                    else:
                        movement_text += f", then {device_df.loc[i, 'country_full']} from {device_df.loc[i, 'start_time']} to {device_df.loc[i, 'end_time']} and spent {device_df.loc[i, 'duration']} hours"
                
                    movement_text += "\n"
                movement_text_all_devices += movement_text +"\n"
    


        else:
        # Convert 'usage_timeframe' to datetime
            table['usage_timeframe'] = pd.to_datetime(table['usage_timeframe'])

            # Identify visits based on changes in 'country_full'
            table['visit_number'] = table.groupby('device_id')['city'].apply(lambda x: (x != x.shift()).cumsum())
            # df.to_csv('test.csv')
            # print(df)
            # Group by 'device_id', 'country_full', and 'visit_id'
            grouped = table.groupby(['device_id', 'city', 'visit_number'])

            # Calculate start and end time for each group
            start_end = grouped.agg(start_time=('usage_timeframe', 'min'), end_time=('usage_timeframe', 'max'))

            # Calculate duration
            start_end['duration'] = start_end['end_time'] - start_end['start_time']

            # Reset index to make 'device_id', 'country_full', and 'visit_id' as columns
            start_end.reset_index(inplace=True)

            # print(start_end)

            # Convert 'start_time' and 'end_time' to datetime
            start_end['start_time'] = pd.to_datetime(start_end['start_time'])
            start_end['end_time'] = pd.to_datetime(start_end['end_time'])
            
            def assign_threat_score(row):
                if 'Jdaidet el Matn' in row['city']:
                    return 80
                else:
                    return 0
            start_end['threat_score'] = start_end.apply(assign_threat_score, axis=1)
            threat_score_table = start_end.groupby('device_id')['threat_score'].mean().reset_index()
            threat_score_table = pd.merge(start_end, threat_score_table, on='device_id', suffixes=('', '_y'))

            # Update the 'threat_score' in the first DataFrame with the values from the second DataFrame
            threat_score_table['threat_score'] = threat_score_table['threat_score_y']
            threat_score_table.drop(columns=['threat_score_y'], inplace=True)
            threat_score_table = threat_score_table[threat_score_table['city']=='Jdaidet el Matn']
            print(threat_score_table)
            threat_score_description=''
            for index, row in threat_score_table.iterrows():                
                # Format the information into a string
                description = f"{row['device_id']} has Threat Score of {row['device_id']} for visiting {row['city']} from {row['start_time']} till {row['end_time']} spending {row['duration']} accross {row['visit_number']} visits"
                threat_score_description+=description
            
            # Create Gantt chart
            fig = px.timeline(start_end, x_start='start_time', x_end='end_time', y='device_id', color='city',
                            title='Device Stays in Each Country',
                            labels={'start_time': 'Start Time', 'end_time': 'End Time', 'city': 'City'})

            fig.update_yaxes(categoryorder='total ascending')  # Ensure correct ordering of devices
            fig.update_layout(self.figure_layout,
            title=dict(
                text='Duration at Location per Day of Week',
                font=dict(family="Bahnschrift SemiBold", size=20, color='#002147'),
                x=0.5, # Center the title horizontally
                xanchor='center' # Align the title's center with the x position
            ))
            fig.write_image(self.properties.passed_filepath_reports_png + "plot.png", format='png', width=1000, height=500, scale=1, engine='kaleido')

            movement_text_all_devices = ""
            for device_id in start_end['device_id'].unique():
                device_df = start_end[start_end['device_id'] == device_id].reset_index(drop=True)
                
                movement_text = f"Device {device_id} moved between cities: "
                if len(device_df) >=10:
                    num_devices = 10
                else:
                    num_devices = len(device_df)
                # Iterate over rows to generate movement details
                for i in range(num_devices):
                    if i == 0:
                        movement_text += f"{device_df.loc[i, 'city']} from {device_df.loc[i, 'start_time']} to {device_df.loc[i, 'end_time']} and spent {device_df.loc[i, 'duration']} hours"
                    else:
                        movement_text += f", then {device_df.loc[i, 'city']} from {device_df.loc[i, 'start_time']} to {device_df.loc[i, 'end_time']} and spent {device_df.loc[i, 'duration']} hours"
                
                    movement_text += "\n"
                movement_text_all_devices += movement_text +"\n"
            print(movement_text_all_devices)
        return movement_text_all_devices ,fig , threat_score_table, threat_score_description
        


    def get_common_locations(self,table:pd.DataFrame=None):
        table = self.utils.binning(table)
        common_places =table[['device_id','usage_timeframe','city','country_full', 'latitude_grid','longitude_grid']]
        # Assuming your DataFrame is named 'df'
        unique_df = common_places.drop_duplicates(subset=['device_id', 'latitude_grid', 'longitude_grid'])

        # Group the unique DataFrame by 'latitude_grid' and 'longitude_grid'
        grouped = unique_df.groupby(['latitude_grid', 'longitude_grid'])['device_id'].agg(['count', list]).reset_index()

        # Rename the columns for clarity
        grouped.columns = ['latitude_grid', 'longitude_grid', 'device_count', 'device_ids']
        grouped = grouped[grouped['device_count'] > 1]
        # Sort the DataFrame by device_count in descending order
        grouped = grouped.sort_values(by='device_count', ascending=False)
        grouped.reset_index(inplace=True,drop=True)
        head_grouped = grouped.head()
        common_grids_all = ""
        fig = self.number_of_device_per_location(head_grouped)
        # Iterate over each row of the DataFrame
        for index, row in head_grouped.iterrows():
            # Extract the necessary information
            latitude = row['latitude_grid']
            longitude = row['longitude_grid']
            device_count = row['device_count']
            device_ids = ', '.join(row['device_ids'])
            
            # Format the information into a string
            description = f"At grid location ({latitude}, {longitude}), there are {device_count} devices with IDs: {device_ids}."
            
            # Append the formatted string to the list
            common_grids_all += description +"\n"
        print(grouped)
        try:
            m = folium.Map(location=[grouped['latitude_grid'].mean(),grouped['longitude_grid'].mean()], zoom_start=10)
        except:
            m = folium.Map( zoom_start=10)

        for index, row in grouped.iterrows():

            folium.Circle(
                [row['latitude_grid'], row['longitude_grid']],
                radius=100,  # This is now in meters
                color='cornflowerblue',
                fill=True,
                fill_opacity=0.6,
                opacity=1,
                popup=f'Number OF Devices: {row["device_count"]}'
            ).add_to(m)        
        return common_grids_all , fig , m 


    def number_of_device_per_location(self, data):
        # Aggregate data by Month and count the number of hits
        data['location'] = list(zip(data['latitude_grid'], data['longitude_grid']))
        data['location'] = data['location'].astype(str)
        print(data)

        fig = px.bar(data, x='location', y='device_count', title='Number of Devices per Location',
                    color='location', color_discrete_sequence=self.months_color_palette,
                    text='device_count')  # Use 'NumberOfHits' column for text labels

        # Layout for title and axes font
        fig.update_layout(
            title={
                'text': 'Number of Devices per Location',
                'font': self.title_font
            },
            xaxis_title_text='Location',  # X-axis title
            yaxis_title_text='Number of Devices',  # Y-axis title
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

        fig.write_image(self.properties.passed_filepath_reports_png + "plot2.png", format='png', width=1000, height=500, scale=1, engine='kaleido')

        return fig
    
#############################################################################################################################################


    def get_number_of_hits_per_dow(self,data):
        
        # Convert 'Timestamp' to datetime and extract day of week
        data['DayOfWeek'] = pd.to_datetime(data['usage_timeframe']).dt.day_name()

        # Count hits per day of the week
        hits_per_day = data['DayOfWeek'].value_counts().reset_index()
        hits_per_day.columns = ['DayOfWeek', 'NumberOfHits']

        # Ensure all days of the week are present, even if no hits
        order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        all_days = pd.DataFrame({'DayOfWeek': order})
        hits_per_day = pd.merge(all_days, hits_per_day, how='left', on='DayOfWeek').fillna(0)
        # Create bar plot with text labels inside the bars
        fig = px.bar(hits_per_day, x='DayOfWeek', y='NumberOfHits', title='Number of Hits per Day of the Week',
                    labels={'DayOfWeek': 'Day of Week', 'NumberOfHits': 'Number of Hits'},
                    text='NumberOfHits')  # Use 'NumberOfHits' column for text labels

        # Layout for title and axes font
        fig.update_layout(
            title={
                'text': 'Number of Hits per Day of the Week',
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

        # Highest number of hits and its respective day
        max_hits_per_dow = hits_per_day['NumberOfHits'].max()
        dow_max_hits = hits_per_day.loc[hits_per_day['NumberOfHits'] == max_hits_per_dow, 'DayOfWeek'].values[0]

        # Lowest number of hits and its respective day
        min_hits_per_dow = hits_per_day['NumberOfHits'].min()
        dow_min_hits = hits_per_day.loc[hits_per_day['NumberOfHits'] == min_hits_per_dow, 'DayOfWeek'].values[0]
        
        # Create tuple of dictionaries for corresponding values and their days
        dow_report_info = [
            {'value': min_hits_per_dow, 'day': dow_min_hits},
            {'value': max_hits_per_dow, 'day': dow_max_hits}
        ]
        daily_hits_list_description = ''
        for _, row in hits_per_day.iterrows():
            daily_hits_list_description += f"Day {row['DayOfWeek']} has a total of {row['NumberOfHits']} hits.\n"
        daily_hits_list_description += f"with the {dow_max_hits} having the most activity with {max_hits_per_dow} hits and {min_hits_per_dow} having the lowest activity with {min_hits_per_dow}"
        fig.write_image(self.properties.passed_filepath_reports_png + "dow_image.png", format='png', width=1000, height=500, scale=1, engine='kaleido')
        
        return daily_hits_list_description ,fig
    
#############################################################################################################################################

    def get_time_spent(self,table):

        table = self.utils.binning(table)
        table = self.utils.combine_coordinates(table)
        table = table.sort_values(['device_id','usage_timeframe'])
        number_of_visits = pd.concat([self.utils.get_visit_df(group.reset_index(drop=True)) for _, group in table.groupby('device_id')])
        number_of_visits['start_time'] = pd.to_datetime(number_of_visits['start_time'])
        number_of_visits['end_time'] = pd.to_datetime(number_of_visits['end_time'])
        # Calculate duration
        number_of_visits['duration'] = number_of_visits['end_time'] - number_of_visits['start_time']

        # Group by device_id and sum duration
        time_spent = number_of_visits.groupby('device_id')['duration'].sum()
        # time_spent = time_spent.sort_values(ascending=False)
        # time_spent = time_spent[time_spent != pd.Timedelta(0)]
        time_spent_df = time_spent.to_frame()

        num_days = (number_of_visits.groupby('device_id')['end_time'].max() - number_of_visits.groupby('device_id')['start_time'].min()).dt.days + 1
        num_days_df = num_days.to_frame()
        num_days_df.reset_index(inplace=True)
        # Calculate average time spent per day for each device
        average_time_per_day = time_spent / num_days
        
        average_time_per_day_df = average_time_per_day.to_frame()
        average_time_per_day_df.reset_index(inplace=True)

        time_spent_df ['numb_of_days'] =  num_days_df[0].values
        time_spent_df ['average_time_per_day'] =  average_time_per_day_df[0].values
        time_spent_df ['average_time_per_day'] =  round(time_spent_df ['average_time_per_day'].dt.total_seconds()/3600)
        time_spent_df['duration_seconds'] = pd.to_timedelta(time_spent_df['duration']).dt.total_seconds()

        # Sort the DataFrame based on duration_seconds in descending order
        time_spent_df = time_spent_df.sort_values(by='duration_seconds', ascending=False)

        # Drop the temporary column
        time_spent_df.drop(columns=['duration_seconds'], inplace=True)
        time_spent_df = time_spent_df[time_spent_df['duration'] != pd.Timedelta(0)]
        time_spent_df.reset_index(inplace=True)
        print(time_spent_df)
        time_spent_list_description= ''

        for index , row in time_spent_df.iterrows():
            if index >=19:
                break
            time_spent_list_description += f"The {row['device_id']} has spent {row['numb_of_days']} Days in this AOI from the total possible days {row['numb_of_days']} ave  averaging {row['average_time_per_day']} hours per day.\n"

        return time_spent_df , time_spent_list_description
    


    def get_hourly_active_device(self, table):
        # create a list to store the results
        results = []
        table['usage_timeframe'] = pd.to_datetime(table['usage_timeframe'])
        # Group the data by hour
        hourly_data = table.groupby(table['usage_timeframe'].dt.hour)

        # Iterate through each unique hour and find devices active at that hour
        for hour, group in hourly_data:
            grouped_counts = group.groupby('device_id').count()

            # Sort the counts
            sorted_counts = grouped_counts.sort_values('location_latitude', ascending=False)

            # Take the top 5
            top_5 = sorted_counts.head(5)

            # Reset the index to convert to DataFrame
            df = top_5.reset_index()

            active_devices = ', '.join(group['device_id'].astype(str).unique().tolist())
            number_if_active_devices = group['device_id'].nunique()
            top_5_devices = df['device_id'].tolist()
            results.append({'Hour': hour, 
                            'Active Devices': active_devices,
                            'Number Of Active Devices':number_if_active_devices,
                            'Top 5 Devices':top_5_devices})

        # convert the list of dictionaries to a DataFrame
        result_df = pd.DataFrame(results)
        # print(result_df)
        hourly_data_list_description =''
        for index , row in result_df.iterrows():
            hourly_data_list_description += f"The {row['Hour']} Hour has {row['Number Of Active Devices']} active devices in this AOI with the most active devices are {row['Top 5 Devices']}.\n"
        # print(hourly_data_list_description)
        # plot the graph
        # Plot the graph using Plotly Express
        fig = px.bar(result_df, x='Hour', y='Number Of Active Devices', title='Number of Active Devices by Hour of the Day',
                    labels={'Number Of Active Devices': 'Number of Active Devices', 'Hour': 'Hour of the Day'})

        # Customize layout if needed
        fig.update_layout(self.figure_layout , xaxis_title='Hour of the Day', yaxis_title='Number of Active Devices')

        # plt.savefig(self.properties.passed_filepath_reports_png + "hourly_activity_image.png")

        return result_df , hourly_data_list_description , fig
    # def get_hourly_active_device(self, table):
    #     # create a list to store the results
    #     fig_list = []
    #     hourly_data_list_description_list= []
    #     result_df_list = []
    #     table['usage_timeframe'] = pd.to_datetime(table['usage_timeframe'])
    #     # Group the data by hour
    #     hourly_data = table.groupby(table['usage_timeframe'].dt.hour)
    #     terminals =['QT-QATAR-Terminal2', 'QT_QATAR-Terminal2-EMIRI', 'QT_QATAR-Terminal1']
    #     for terminal in terminals:
    #         results = []
    #         hourly_data_list_description =''

    #         # Iterate through each unique hour and find devices active at that hour
    #         for hour, group in hourly_data:
    #             grouped_counts = group.groupby('device_id').count()

    #             # Sort the counts
    #             sorted_counts = grouped_counts.sort_values('location_latitude', ascending=False)

    #             # Take the top 5
    #             top_5 = sorted_counts.head(5)

    #             # Reset the index to convert to DataFrame
    #             df = top_5.reset_index()

    #             active_devices = ', '.join(group['device_id'].astype(str).unique().tolist())
    #             number_if_active_devices = group['device_id'].nunique()
    #             top_5_devices = df['device_id'].tolist()
    #             results.append({'Hour': hour, 
    #                             'Number Of Active Devices': random.randint(20, 880)})

    #         # convert the list of dictionaries to a DataFrame
    #         result_df = pd.DataFrame(results)
    #         # print(result_df)
    #         for index , row in result_df.iterrows():
    #             hourly_data_list_description += f"The {row['Hour']} Hour has {row['Number Of Active Devices']} active devices in this AOI.\n"
    #         # print(hourly_data_list_description)
    #         # plot the graph
    #         # Plot the graph using Plotly Express
    #         fig = px.bar(result_df, x='Hour', y='Number Of Active Devices', title='Number of Active Devices by Hour of the Day',
    #                     labels={'Number Of Active Devices': 'Number of Active Devices', 'Hour': 'Hour of the Day'})

    #         # Customize layout if needed
    #         fig.update_layout(self.figure_layout , xaxis_title='Hour of the Day', yaxis_title='Number of Active Devices')
    #         fig_list.append(fig)
    #         hourly_data_list_description_list.append(hourly_data_list_description)
    #         result_df_list.append(result_df)
    #     # plt.savefig(self.properties.passed_filepath_reports_png + "hourly_activity_image.png")
    #     print(hourly_data_list_description_list)
    #     return result_df_list , hourly_data_list_description_list , fig_list

    def get_geofencing (self, table, start_date, end_date):
        start_date = '2023-09-25'
        end_date = '2024-01-14'
        start_date = self.utils.convert_datetime_to_ms(str(start_date))
        end_date = self.utils.convert_datetime_to_ms(str(end_date))
        devices_list = table['device_id'].unique().tolist()
        print(f'Number of devices {len(devices_list)}')
        region = 142
        sub_region = 145
        server = '10.1.2.205'
        df_history = self.cassandra_tools.get_device_history_geo_chunk(devices_list, start_date, end_date,region , sub_region , server)
        df_history = self.utils.add_reverseGeocode_columns(df_history)
        # df_history.to_csv('test.csv')
        grouped_geofencing = df_history[['device_id','country']].drop_duplicates()
        def assign_threat_score(row):
            if 'SA' in row['country']:
                return 80
            else:
                return 0

        # Apply the function to each row in the DataFrame
        grouped_geofencing['threat_score'] = grouped_geofencing.apply(assign_threat_score, axis=1)
        grouped_geofencing = grouped_geofencing.groupby('device_id')['threat_score'].sum().reset_index()
        grouped_geofencing = grouped_geofencing.sort_values(by='threat_score', ascending=False).reset_index(drop=True)
        grouped_geofencing = grouped_geofencing[grouped_geofencing['threat_score']> 0]
        devices_list_geofencing = grouped_geofencing['device_id'].unique().tolist()
        print(grouped_geofencing)
        
        filtered_df = df_history[df_history['device_id'].isin(devices_list_geofencing)]
        return grouped_geofencing