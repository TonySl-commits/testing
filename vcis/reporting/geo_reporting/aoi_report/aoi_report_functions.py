##############################################################################################################################
    # Imports
##############################################################################################################################

import calendar
import ast
import folium
import pandas as pd
import numpy as np
import osmnx as ox
import networkx as nx
import plotly.io as pio
import plotly.express as px
import plotly.graph_objects as go

from folium.plugins import TimestampedGeoJson, AntPath
from geopy.distance import geodesic
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

from datetime import datetime, timedelta

from vcis.aoi_classification.aoi_support_functions import reverse_geocoding
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.aoi_classification.geolocation_data import GeoLocationData
from vcis.utils.utils import CDR_Properties, CDR_Utils
from vcis.reporting.pdf_reporting.prompt_properties import PromptProperties

class AOIReportFunctions():
    def __init__(self):
        self.geolocation_data : GeoLocationData
        self.cassandra_tools = CassandraTools()
        self.properties = CDR_Properties()
        self.utils = CDR_Utils()
        self.locations_df =  pd.DataFrame()
        self.aoi_dataframe_result = pd.DataFrame()
        self.location_address_mapping = {'Other': 'Other'}
        self.dow_color_palette = ['#03045E', '#023E8A', '#0077B6', '#0096C7', '#00B4D8','#48CAE4', '#90E0EF', '#ADE8F4', '#CAF0F8']
        self.months_color_palette = ['#1130A7', '#125F92', '#128F7E', '#13BE69', '#13EE55', '#A4D82F','#ECCD1C', '#FC9C14', '#CB620F', '#9A270A', '#561463', '#1100BB']
        self.figure_layout = {'paper_bgcolor': 'rgba(255, 255, 255, 1)', 'plot_bgcolor': 'rgba(255, 255, 255, 1)'}
        self.title_font = dict(family="Bahnschrift SemiBold", size=20, color='#03045E')
        self.axes_font = dict(family="Bahnschrift SemiBold", size=15, color='#03045E')       
        self.current_time = datetime.now()
        self.order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        self.trajectory_displayed = False
        self.prompt_properties = PromptProperties()

##############################################################################################################################
    # Reporting Functions
##############################################################################################################################       
    def initialize_aoi_report(self, geolocation_data):
        self.geolocation_data = geolocation_data

    def get_location_addresses(self):
        result_AOI = self.aoi_dataframe()

        aoi_locations = result_AOI.copy(deep=True)
        
        # Create a new column 'ADDRESS' initialized with empty strings
        aoi_locations['ADDRESS'] = ''
        
        # Use str.contains() to check if 'LOCATION' contains 'Undetermined'
        undetermined_mask = aoi_locations['AOI TYPE'].str.contains('Undetermined')
        
        # Populate 'ADDRESS' based on the condition
        aoi_locations.loc[undetermined_mask, 'ADDRESS'] = aoi_locations.loc[undetermined_mask, 'LOCATION']
        aoi_locations.loc[~undetermined_mask, 'ADDRESS'] = aoi_locations.loc[~undetermined_mask, 'AOI TYPE'] + ': ' + aoi_locations.loc[~undetermined_mask, 'LOCATION']
        
        # Create new column COORDS combining LAT and LNG in a tuple
        aoi_locations['COORDS'] = '(' + aoi_locations['LATITUDE'].astype(str) + ', ' + aoi_locations['LONGITUDE'].astype(str) + ')'

        # Create a dictionary from COORDS as keys and ADDRESS as values
        location_address_mapping = aoi_locations.set_index('COORDS')['ADDRESS'].to_dict() 

        for key, value in location_address_mapping.items():
            self.location_address_mapping[key] = value

    def update_location_address_mapping(self, new_locations):
        for point in new_locations:
            # Convert point string to tuple
            new_point = ast.literal_eval(point)

            # Do reverse geocoding to get the address
            value = reverse_geocoding(new_point)

            self.location_address_mapping[point] = value

    def ensure_location_mapping(self, df):
        # Get unique locations in the DataFrame
        locations = df['Location'].unique()

        # Find all unique locations in the DataFrame that are not in the mapping
        available_addresses = list(self.location_address_mapping.keys())
        missing_locations = {loc for loc in locations if loc not in available_addresses and loc != 'Other'}

        # Update mapping for missing locations
        if len(missing_locations) > 0:
            self.update_location_address_mapping(missing_locations)

        # Map the updated locations back to addresses
        df['Address'] = df['Location'].map(self.location_address_mapping.get)

        # Strip long location addresses
        df['Address'] = df['Address'].apply(self.strip_long_location_address)
        
        return df
    
    def strip_long_location_address(self, input_string):
        # Split the string by commas
        parts = input_string.split(',')

        # Check the number of parts produced by the split
        if len(parts) < 3:
            # If there are less than three parts, return the entire string
            return input_string
        else:
            # If there are at least three parts, return the substring before the second comma
            return ','.join(parts[:2])


    def get_locations_df(self):
        if not self.locations_df.empty:
            return self.locations_df
        else:
            data = self.geolocation_data.data.copy(deep=True)
            no_outliers = self.geolocation_data.no_outliers.copy(deep=True)

            merge_columns = ['Device_ID_geo', 'Location_Name', 'Service_Provider_ID', 'Latitude','Longitude', 'Timestamp']
            locations_df = pd.merge(data, no_outliers, how='left', on=merge_columns)
            locations_df.drop(columns=['index', 'Day of week', 'Date', 'cluster_label', 'Cluster'], inplace=True)
            
            locations_df['cluster_label_centroid'] = locations_df['cluster_label_centroid'].fillna('Other').astype(str)
            locations_df.rename(columns={'cluster_label_centroid': 'Location'}, inplace=True)

            # Convert 'Timestamp' to datetime
            locations_df['Timestamp'] = pd.to_datetime(locations_df['Timestamp'])

            # Extract day of week, day of month and hour of day
            locations_df['DayOfWeek'] = locations_df['Timestamp'].dt.day_name()
            locations_df['DayOfMonth'] = locations_df['Timestamp'].dt.day
            locations_df['HourOfDay'] = locations_df['Timestamp'].dt.hour
            locations_df['Date'] = locations_df['Timestamp'].dt.date
            
            self.locations_df = locations_df
            return locations_df
    
    def number_of_hits_per_dow(self):
        data = self.geolocation_data.data

        # Convert 'Timestamp' to datetime and extract day of week
        data['DayOfWeek'] = pd.to_datetime(data['Timestamp']).dt.day_name()

        # Count hits per day of the week
        hits_per_day = data['DayOfWeek'].value_counts().reset_index()
        hits_per_day.columns = ['DayOfWeek', 'NumberOfHits']

        # Ensure all days of the week are present, even if no hits
        order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        all_days = pd.DataFrame({'DayOfWeek': order})
        hits_per_day = pd.merge(all_days, hits_per_day, how='left', on='DayOfWeek').fillna(0)

        hits_per_day_description = ''
        for _, row in hits_per_day.iterrows():
            hits_per_day_description += f"Day {row['DayOfWeek']} has a total of {row['NumberOfHits']} hits. "

        self.prompt_properties.prompt["hits_per_dow"]["user"] = hits_per_day_description

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

        # Create the description for the report
        hits_per_dow_description = f"This plot shows a minimum of {dow_report_info[0]['value']} hits on {dow_report_info[0]['day']} and a maximum of {dow_report_info[1]['value']} of hits on {dow_report_info[1]['day']}"

        # pio.write_image(fig = fig, width = 1200, height = 420, file = self.properties.passed_filepath_reports_png + 'number_of_hits_per_dow.jpg', format='jpg')

        # self.prompt_properties.prompt['hits_per_dow']['figure'] = self.properties.passed_filepath_reports_png + 'number_of_hits_per_dow.jpg'
        
        # Create a dictionary for the figure block
        figure_block = {
            'BLOCK_CONTENT': fig,
            'BLOCK_NAME': 'Number of Hits Per Day of Week',
            'BLOCK_TYPE': 'PLOTLY',
            'BLOCK_ASSOCIATION': 'AOI',
            'DEVICE_ID': self.geolocation_data.device_id_geo
        }

        # Create a dictionary for the figure description
        description_block = {
            'BLOCK_CONTENT': hits_per_dow_description,
            'BLOCK_NAME': 'Number of Hits Per Day of Week Report Description',
            'BLOCK_TYPE': 'TEXT',
            'BLOCK_ASSOCIATION': 'AOI',
            'DEVICE_ID': self.geolocation_data.device_id_geo
        }

        return figure_block, description_block

    def number_of_hits_per_month(self):
        data = self.geolocation_data.data.copy()
        
        data['Timestamp'] = pd.to_datetime(data['Timestamp'])

        # Generate the year and month string from 'Timestamp'
        data['Month'] = data['Timestamp'].dt.to_period('M')

        # Aggregate data by Month and count the number of hits
        hits_per_month = data.groupby('Month').size().reset_index(name='NumberOfHits')

        # Create a DataFrame for all months in the year
        year = data['Timestamp'].dt.year.iloc[0]  # Use the year from the first record
        all_months = [f"{year}-{month:02d}" for month in range(1, 13)]
        all_months_df = pd.DataFrame({'Month': pd.PeriodIndex(all_months, freq='M')})

        # Merge the aggregated data with the all_months_df DataFrame
        merged_data = pd.merge(all_months_df, hits_per_month, on='Month', how='left').fillna(0)

        # Convert 'Month' to timestamps to extract the month names
        merged_data['Month'] = merged_data['Month'].dt.start_time
        merged_data['MonthName'] = merged_data['Month'].dt.month.apply(lambda x: calendar.month_name[x])

        # Highest number of hits and its respective day
        max_hits = merged_data['NumberOfHits'].max()
        max_day = merged_data.loc[merged_data['NumberOfHits'] == max_hits, 'MonthName'].values[0]

        # Lowest number of hits and its respective day
        min_hits = merged_data['NumberOfHits'].min()
        min_day = merged_data.loc[merged_data['NumberOfHits'] == min_hits, 'MonthName'].values[0]

        # Create tuple of dictionaries for corresponding values and their days
        monthly_hits_info = [
            {'value': min_hits, 'month': min_day},
            {'value': max_hits, 'month': max_day}
        ]

        # monthly_hits_list_description = ''
        # for _, row in merged_data.iterrows():
        #     monthly_hits_list_description += f"Month {row['Month']} has a total of {row['NumberOfHits']} hits. "

        # self.prompt_properties.prompt["hits_per_month"]["user"] = monthly_hits_list_description

        # Create bar plot with text labels inside the bars
        fig = px.bar(merged_data, x='MonthName', y='NumberOfHits', title='Number of Hits per Month',
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

        fig.update_traces(textposition='inside', textfont=dict(color='white')) 

        # pio.write_image(fig = fig, width = 1200, height = 420, file = self.properties.passed_filepath_reports_png + 'number_of_hits_per_month.jpg', format='jpg')

        # self.prompt_properties.prompt['hits_per_month']['figure'] = self.properties.passed_filepath_reports_png + 'number_of_hits_per_month.jpg'

        hits_per_month_description =f"This plot shows a minimum of {monthly_hits_info[0]['value']} hits on {monthly_hits_info[0]['month']} and a maximum of {monthly_hits_info[1]['value']} of hits on {monthly_hits_info[1]['month']}"

        figure_block = {
            'BLOCK_CONTENT': fig,
            'BLOCK_NAME': 'Number of Hits Per Month',
            'BLOCK_TYPE': 'PLOTLY',
            'BLOCK_ASSOCIATION': 'AOI',
            'DEVICE_ID': self.geolocation_data.device_id_geo
        }
        
        description_block = {
            'BLOCK_CONTENT': hits_per_month_description,
            'BLOCK_NAME': 'Number of Hits per Month Report Description',
            'BLOCK_TYPE': 'TEXT',
            'BLOCK_ASSOCIATION': 'AOI',
            'DEVICE_ID': self.geolocation_data.device_id_geo
        }

        return figure_block, description_block

    def location_likelihood_per_dow(self):
        
        """
        Location Likelihood with respect to Day of Week
        """
        locations_df = self.get_locations_df()

        # Group by day of week and calculate location likelihood
        location_likelihood_per_dow = locations_df.groupby(['DayOfWeek', 'Location']).size().unstack(fill_value=0)
        location_likelihood_per_dow = location_likelihood_per_dow.div(location_likelihood_per_dow.sum(axis=1), axis=0) * 100

        # Ensure all days of the week are present in the DataFrame
        order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        all_days_df = pd.DataFrame(order, columns=['DayOfWeek'])
        location_likelihood_per_dow = all_days_df.merge(location_likelihood_per_dow, on='DayOfWeek', how='left').fillna(0)

        # Reshape the DataFrame to long format for plotting
        location_likelihood_per_dow_long = location_likelihood_per_dow.melt(id_vars='DayOfWeek', value_name='Likelihood', var_name='Location')
        location_likelihood_per_dow_long['Text'] = location_likelihood_per_dow_long['Likelihood'].apply(lambda x: f'{x:.1f}%')

        # Ziad's additions start here
        location_likelihood_per_dow_long = location_likelihood_per_dow_long[location_likelihood_per_dow_long['Location'] != 'Unknown']

        # Find the day with highest duration for each location
        max_location_likelihood_per_location = location_likelihood_per_dow_long.groupby(['Location', 'DayOfWeek'])['Likelihood'].max().reset_index()
        day_with_max_location_likelihood = max_location_likelihood_per_location.loc[max_location_likelihood_per_location.groupby('Location')['Likelihood'].idxmax()]

        # Aggregate total duration for each location per day of the week
        total_duration_per_day_location = location_likelihood_per_dow_long.groupby(['DayOfWeek', 'Location']).sum().reset_index()

        # Identify the location with the maximum total duration for each day of the week
        most_prominent_location_per_day = total_duration_per_day_location.loc[total_duration_per_day_location.groupby('DayOfWeek')['Likelihood'].idxmax()]

        # Remove rows where duration is 0
        most_prominent_location_per_day = most_prominent_location_per_day[most_prominent_location_per_day['Likelihood'] != 0]

        # Remove the function and the line after in case original data frame was in tuple format and already reduced
        def reduce_decimal(location_str, decimal_places=8):
            if location_str == 'Other' or 'Unknown' : return location_str
            parts = location_str.strip('()').split(',') # Convert each part to float, round it to desired decimal places, and convert back to string
            reduced_parts = [str(round(float(part), decimal_places)) for part in parts]  # Convert string representation of tuple to a tuple
            reduced_location = '(' + ', '.join(reduced_parts) + ')'
            return str(reduced_location)
        
        most_prominent_location_per_day['Location'] = most_prominent_location_per_day['Location'].apply(lambda x: reduce_decimal(x, decimal_places=8))
        day_with_max_location_likelihood['Location'] = day_with_max_location_likelihood['Location'].apply(lambda x: reduce_decimal(x, decimal_places=8))

        # Sort the DataFrame by day of the week
        day_with_max_location_likelihood['DayOfWeek'] = pd.Categorical(day_with_max_location_likelihood['DayOfWeek'], categories=order, ordered=True)
        day_with_max_location_likelihood = day_with_max_location_likelihood.sort_values(by='DayOfWeek').reset_index(drop=True)
        most_prominent_location_per_day['DayOfWeek'] = pd.Categorical(most_prominent_location_per_day['DayOfWeek'], categories=order, ordered=True)
        most_prominent_location_per_day = most_prominent_location_per_day.sort_values(by='DayOfWeek').reset_index(drop=True)

        # Generating string to add as text under the plot
        day_description = []
        for _, row in day_with_max_location_likelihood.iterrows():
            if row['Location'] is not 'Other': 
                day_description_text = f"The location {row['Location']} is mostly visited on {row['DayOfWeek']} with a likelihood of {row['Likelihood']:.2f}%."
                day_description_text = day_description_text
                day_description.append(day_description_text)

        prominence_description = []
        for _, row in most_prominent_location_per_day.iterrows():
            prominence_description_text = f"On {row['DayOfWeek']} the location {row['Location']} is most likely to be visited with a likelihood of {row['Likelihood']:.2f}%."
            prominence_description_text = prominence_description_text
            if row['Location'] is 'Other' : prominence_description_text = f"On {row['DayOfWeek']} the user is most likely have a high movement activity with a likelihood of {row['Likelihood']:.2f}%."
            prominence_description.append(prominence_description_text)

        likelihood_description = day_description + prominence_description

        # Update Location column based on the mapping dictionary
        location_likelihood_per_dow_long = self.ensure_location_mapping(location_likelihood_per_dow_long)

        # Plot Location Likelihood per day of week
        fig = px.bar(location_likelihood_per_dow_long,
             x='DayOfWeek', y='Likelihood', color='Address', text='Text',
             title='Location Likelihood per Day of Week',
             labels={'DayOfWeek': 'Day of Week', 'Likelihood': 'Likelihood (%)'},
             category_orders={'DayOfWeek': order},
             barmode='stack',
             color_discrete_sequence=self.dow_color_palette)

        # Customize layout
        fig.update_layout(
            self.figure_layout,
            title=dict(text='Location Likelihood per Day of Week', font=self.title_font),
            xaxis=dict(title='Day of Week', titlefont=self.axes_font, tickfont=self.axes_font),
            yaxis=dict(title='Likelihood (%)', titlefont=self.axes_font, tickfont=self.axes_font), 
        )

        # Customize text font color to white inside bars
        fig.update_traces(textposition='inside', textfont=dict(color='white'),
                        marker_line_color='white', marker_line_width=1.5)
        
        likelihood_description = '\n'.join(likelihood_description)
        
        # Create figure block
        figure_block = {
            'BLOCK_CONTENT': fig,
            'BLOCK_NAME': 'Location Likelihood Per Day of Week',
            'BLOCK_TYPE': 'PLOTLY',
            'BLOCK_ASSOCIATION': 'AOI',
            'DEVICE_ID': self.geolocation_data.device_id_geo
        }

        # Create description block        
        description_block = {
            'BLOCK_CONTENT': likelihood_description,
            'BLOCK_NAME': 'Location Likelihood Per Day of Week Description',
            'BLOCK_TYPE': 'TEXT',
            'BLOCK_ASSOCIATION': 'AOI',
            'DEVICE_ID': self.geolocation_data.device_id_geo
        }

        return figure_block, description_block

    
    def likely_location_per_dow_hod(self):
        """
        Calculate the likely location for each day of the week and hour of the day based on historical data. Return the most likely location.
        """
        locations_df = self.get_locations_df()
        locations_df['Timestamp'] = pd.to_datetime(locations_df['Timestamp'])
        locations_df['Date'] = pd.to_datetime(locations_df['Date'])
        locations_df['HourOfDay'] = locations_df['HourOfDay'].astype(int)

        # Create a new DataFrame for the transformation
        transformed_df = pd.pivot_table(locations_df, 
                                        values='Location_Name', 
                                        index=['DayOfWeek', 'HourOfDay'], 
                                        columns='Location', 
                                        aggfunc=lambda x: 1 if x.notna().any() else 0, 
                                        fill_value=0)

        # Reset the index to make 'DayOfWeek' and 'HourOfDay' as regular columns
        transformed_df.reset_index(inplace=True)
        transformed_df['DayOfWeek'] = pd.Categorical(transformed_df['DayOfWeek'], categories=self.order, ordered=True)
        transformed_df.sort_values(by=['DayOfWeek', 'HourOfDay'], inplace=True)

        # Generate a MultiIndex for all possible combinations of DayOfWeek and HourOfDay
        all_combinations_final = pd.MultiIndex.from_product([self.order, range(24)], names=['DayOfWeek', 'HourOfDay'])

        # Reindex to fill missing combinations with zeros
        transformed_df = transformed_df.set_index(['DayOfWeek', 'HourOfDay']).reindex(all_combinations_final, fill_value=0).reset_index()

        # Create a new DataFrame for percentages
        percentages_df = transformed_df.copy()

        # Calculate percentages for each location
        location_columns = transformed_df.columns[2:]
        total_per_row = transformed_df.iloc[:, 2:].sum(axis=1)

        percentages_df[location_columns] = transformed_df[location_columns].div(total_per_row, axis=0) * 100

        # Replace NaN values with 0
        percentages_df.fillna(0, inplace=True)

        # Calculate the average percentages for each day of the week and hour of the day
        average_percentages_df = percentages_df.groupby(['DayOfWeek', 'HourOfDay']).mean().reset_index()

        # Extracting the highest percentage and most likely location
        subset = average_percentages_df.loc[
            (average_percentages_df['DayOfWeek'] == self.order[self.current_time.weekday()]) & (average_percentages_df['HourOfDay'] == self.current_time.hour)]
        current_location = subset.iloc[:, 2:].idxmax(axis=1).values[0]
        return current_location

    def get_average_duration_at_AOI_per_DOW(self):
        locations_df = self.get_locations_df()

        # Sort the DataFrame by 'Device_ID_geo' and 'Timestamp'
        locations_df = locations_df.sort_values(by=['Device_ID_geo', 'Timestamp'])

        # Group by 'Device_ID_geo', 'DayOfWeek', 'Location', and consecutive changes in 'Location'
        groups = (locations_df['Location'] != locations_df['Location'].shift()).cumsum()

        # Group by the calculated groups and aggregate the results
        result_df = locations_df.groupby(['Device_ID_geo', 'DayOfWeek', 'Location', groups], as_index=False).agg(
            start_time=('Timestamp', 'first'),
            end_time=('Timestamp', 'last')
        )

        # Creating a date column
        result_df['start_time'] = pd.to_datetime(result_df['start_time'])
        result_df['end_time'] = pd.to_datetime(result_df['end_time'])
        result_df['Date'] = result_df['start_time'].dt.date

        # Create a new column for the hour of the day
        result_df['HourOfDay'] = result_df['start_time'].dt.hour

        # Calculating the stay duration per read
        result_df['duration'] = result_df['end_time'] - result_df['start_time']

        # Convert the duration to seconds for better readability
        result_df['duration_minutes'] = result_df['duration'].dt.total_seconds() / 60

        # Calculating the total duration of the Device in this location per DOW, Date, HOD
        total_duration_per_HOD_DOW = result_df.groupby(['Device_ID_geo','Date','DayOfWeek','Location','HourOfDay'])['duration_minutes'].sum().reset_index()

        # Calculating the average duration over all dates per DOW, HOD
        average_duration_over_dates = total_duration_per_HOD_DOW.groupby(['Device_ID_geo','DayOfWeek','Location','HourOfDay'])['duration_minutes'].mean().reset_index()

    def get_expected_duration_at_AOI_per_stay(self):
        # Get locations_df, format and sort it
        locations_df = self.get_locations_df()
        locations_df['Timestamp'] = pd.to_datetime(locations_df['Timestamp'])
        locations_df = locations_df.sort_values(by=['Device_ID_geo', 'Timestamp']).drop_duplicates().reset_index(drop=True)

        # Initialize the variables for the loop
        location = locations_df.iloc[0]['Location']
        last_index = 0
        sliced_dfs = []

        # Slice the data frame into smaller data frames, grouping the discrete locations
        for index, row in locations_df.iterrows():
            if row['Location'] != location:
                sliced_df = locations_df[last_index:index]
                sliced_df = sliced_df.groupby(['Device_ID_geo','Location']).agg(
                    Latitude = ('Latitude', 'mean'),
                    Longitude = ('Longitude','mean'),
                    start_time=('Timestamp', 'first'),
                    end_time=('Timestamp', 'last')
                ).reset_index()

                # Calculating the distance and duration
                location_tuple = (locations_df.loc[last_index, 'Latitude'], locations_df.loc[last_index, 'Longitude'])
                current_location = (sliced_df.iloc[0]['Latitude'], sliced_df.iloc[0]['Longitude'])
                sliced_df['duration_minutes'] = sliced_df.apply(lambda row: (sliced_df.iloc[0]['end_time'] - sliced_df.iloc[0]['start_time']).total_seconds() / 60, axis=1)
                sliced_df['distance'] = sliced_df.apply(lambda row: geodesic(location_tuple, current_location).kilometers, axis=1)

                # Check if duration less than 5 minutes and distance less than 0.1 km, rename to the previous location
                if sliced_df.iloc[0]['Location'] == 'Other' and sliced_df.iloc[0]['duration_minutes'] < 5 and sliced_df.iloc[0]['distance'] < 0.1:
                    sliced_df.at[0, 'Location'] = locations_df.iloc[last_index - 1]['Location']

                sliced_dfs.append(sliced_df)
                last_index = index
                location = row['Location']

        # Finalize the data frame by concatinating and rounding the values
        final_df = pd.concat(sliced_dfs).reset_index(drop=True)

        # Restart in order to group up the slices according to location
        location = final_df.iloc[0]['Location']
        last_index = 0
        sliced_dfs = []

        for index, row in final_df.iterrows():
            if row['Location'] != location :
                sliced_df = final_df.iloc[last_index: index]
                sliced_df = sliced_df.groupby(['Device_ID_geo','Location']).agg({
                    'Latitude': 'mean',
                    'Longitude' : 'mean',
                    'distance' : 'mean',
                    'start_time' : 'first',
                    'end_time' : 'last'
                }).reset_index()

                last_index = index
                sliced_dfs.append(sliced_df)
                location = row['Location']

        if index + 1 == len(final_df):
            last_row = final_df.loc[index:index]
            sliced_dfs.append(last_row)

        final_df = pd.concat(sliced_dfs).reset_index(drop = True)
        final_df['duration_minutes'] = round((final_df['end_time'] - final_df['start_time']).dt.total_seconds() / 60.0, 2)
        final_df['distance'] = round(final_df['distance'],2)

        current_location = self.likely_location_per_dow_hod()

        # Check if system_time falls within any time interval for the specified location
        interval_mask = (final_df['Location'] == current_location) & (final_df['start_time'].dt.time <= self.current_time.time()) & (final_df['end_time'].dt.time >= self.current_time.time())

        if interval_mask.any():
            # Calculate average duration for the interval
            expected_duration = final_df.loc[interval_mask, 'duration_minutes'].mean()
            return final_df, expected_duration, current_location

        expected_duration = "Data availabe is not enough"
        return final_df, expected_duration, current_location

    def prepare_data_to_predict_next_location(self):
        """
        Prepares the data for analysis by generating an itinerary DataFrame and retrieving expected durations and likely locations.

        Returns:
        - itinerary_df (DataFrame): A DataFrame containing the current AOI, hour of day, weekend status, and next AOI for each stay.
        - expected_duration (float): The expected duration of each stay at the AOI.
        - likely_location (str): The most likely location for each stay.
        """
        expected_durations_df, expected_duration, current_location = self.get_expected_duration_at_AOI_per_stay()
        # print(expected_durations_df['Location'])

        itinerary_df = pd.DataFrame(columns = ['CurrentAOI','DayOfWeek', 'HourOfDay','Weekend','NextAOI'])
        index = 0

        while index + 1 < len(expected_durations_df):
            if expected_durations_df.iloc[index]['Location'] != 'Other':
                new_row1 = {
                    'CurrentAOI' : expected_durations_df.iloc[index]['Location'],
                    'HourOfDay': round((expected_durations_df.iloc[index]['start_time'].hour + expected_durations_df.iloc[index]['end_time'].hour) / 2),
                    'DayOfWeek': expected_durations_df.iloc[index]['start_time'].weekday(),
                    'Weekend': 0
                }

                if new_row1['HourOfDay'] > 24:
                    new_row1['DayOfWeek'] = (expected_durations_df.iloc[index]['start_time'].weekday() % 7) + 1
                    new_row1['HourOfDay'] = int(round(new_row1['HourOfDay'] % 24))

                if expected_durations_df.iloc[index + 1]['Location'] == 'Other' and index + 1 != len(expected_durations_df) - 1:
                    new_row2 = {
                        'NextAOI': expected_durations_df.iloc[index + 2]['Location'],
                    }
                    index += 1

                else:
                    new_row2 = {
                        'NextAOI': expected_durations_df.iloc[index + 1]['Location'],
                    }
                new_row1['Weekend'] = np.where(new_row1['DayOfWeek'] in [4, 6], 1, 0)
                new_row1.update(new_row2)
                itinerary_df = pd.concat([itinerary_df, pd.DataFrame([new_row1])], ignore_index=True)

            index += 1

        # Initialize an empty dictionary to store counts
        counts = {'CurrentAOI': [], 'HourOfDay': [], 'Weekend': [], 'NextAOI': []}
        for day in range(7):
            counts[f'DayOfWeek_{day}'] = []

        # Iterate over DataFrame rows
        for index, row in itinerary_df.iterrows():
            counts['CurrentAOI'].append(row['CurrentAOI'])
            counts['HourOfDay'].append(row['HourOfDay'])
            counts['Weekend'].append(row['Weekend'])
            counts['NextAOI'].append(row['NextAOI'])

            # Count occurrences for each day of the week
            for day in range(7):
                counts[f'DayOfWeek_{day}'].append(1 if row['DayOfWeek'] == day else 0)

        # Create a new DataFrame from the dictionary
        itinerary_df = pd.DataFrame(counts)

        # Rename the columns to represent Monday through Sunday
        day_mapping = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
        itinerary_df.columns = ['CurrentAOI', 'HourOfDay', 'Weekend', 'NextAOI'] + [f'{day_mapping[day]}' for day in range(7)]

        return itinerary_df, expected_duration, current_location

    def next_likely_location(self):
        """
        Get the necessary data, split into features and target, train model, evaluate accuracy, use trained model to predict next location, format and return the results.
        """
        # Get the necessary data
        df, expected_duration, current_location = self.prepare_data_to_predict_next_location()
        df = df[df['CurrentAOI'] == current_location]
        df = df.drop(['CurrentAOI'], axis=1).reset_index(drop=True)

        # Split into features and target
        X = df[['HourOfDay', 'Weekend', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']]
        y = df['NextAOI']

        if len(X) < 5:
            self.trajectory_displayed = False
            return current_location, 'Not enough data available', 0, expected_duration

        # Split and train model
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Use decision tree to predict the next location
        dt_classifier = DecisionTreeClassifier()
        dt_classifier.fit(X_train, y_train)
        y_pred = dt_classifier.predict(X_test)
        accuracy = round(accuracy_score(y_test, y_pred),2)

        # Use the trained model to predict the next location
        test_data = pd.DataFrame({
            'HourOfDay': [self.current_time.hour],
            'Weekend': [self.current_time.weekday() >= 5],
            'Monday': [self.current_time.strftime('%A') == 'Monday'],
            'Tuesday': [self.current_time.strftime('%A') == 'Tuesday'],
            'Wednesday': [self.current_time.strftime('%A') == 'Wednesday'],
            'Thursday': [self.current_time.strftime('%A') == 'Thursday'],
            'Friday': [self.current_time.strftime('%A') == 'Friday'],
            'Saturday': [self.current_time.strftime('%A') == 'Saturday'],
            'Sunday': [self.current_time.strftime('%A') == 'Sunday']
        })
        y_pred = dt_classifier.predict(test_data)
        potential_next_location = y_pred[0]

        # Format and return the results REMOVE THE NEXT TWO LINES IF WORKING WITH NEW_GEOLOCATION_DATA
        values = potential_next_location.strip('()').split(',')
        rounded_values = [f'{round(float(value), 8)}' for value in values]
        potential_next_location = f'({rounded_values[0]}, {rounded_values[1]})'

        values = current_location.strip('()').split(',')
        rounded_values = [f'{round(float(value), 8)}' for value in values]
        current_location = f'({rounded_values[0]}, {rounded_values[1]})'

        self.trajectory_displayed = True
        if isinstance(expected_duration, str) or potential_next_location == current_location or accuracy == 1:
            self.trajectory_displayed = False

        return current_location, potential_next_location, accuracy, expected_duration 

    def get_location_duration_per_day(self):
        locations_df = self.get_locations_df()
        device_id_geo = self.geolocation_data.device_id_geo
        
        # Initialize the output dataframe
        stays_per_day = pd.DataFrame(columns=['DeviceID', 'Date', 'Location', 'DurationOfStay', 'MinHour', 'MaxHour', 'FirstHit', 'LastHit'])

        # loop through each date for the current device ID
        for date in locations_df['Date'].unique():
            # initialize variables to track the current location and stay information
            current_location = locations_df.loc[(locations_df['Date'] == date), 'Location'].iloc[0]
            stay_start_time = locations_df.loc[(locations_df['Date'] == date), 'Timestamp'].iloc[0]
            stay_end_time = stay_start_time
            stay_duration = timedelta(0)

            # loop through each row of data for the current device ID and date
            for _, row in locations_df.loc[(locations_df['Date'] == date)].iterrows():

                # Check if the location has changed
                if row['Location'] != current_location:

                    # Calculate the duration of the previous stay and update the output dataframe
                    stay_duration = stay_end_time - stay_start_time
                    new_row = {'DeviceID': device_id_geo,
                            'Date': date,
                            'Location': current_location,
                            'DurationOfStay': stay_duration.total_seconds() / 3600,
                            'MinHour': stay_start_time.hour,
                            'MaxHour': stay_end_time.hour, 
                            'FirstHit': stay_start_time,
                            'LastHit': stay_end_time}

                    stays_per_day.loc[len(stays_per_day)] = new_row

                    # Update the current location and stay information
                    current_location = row['Location']
                    stay_start_time = row['Timestamp']
                    stay_end_time = stay_start_time
                    stay_duration = timedelta(0)

                # Update the end time for the current stay
                stay_end_time = row['Timestamp']

            # Calculate the duration of the final stay and update the output dataframe
            stay_duration = stay_end_time - stay_start_time
            new_row = {
                'DeviceID': device_id_geo,
                'Date': date,
                'Location': current_location,
                'DurationOfStay': stay_duration.total_seconds() / 3600,
                'MinHour': stay_start_time.hour,
                'MaxHour': stay_end_time.hour,
                'FirstHit': stay_start_time,
                'LastHit': stay_end_time
            }
            
            stays_per_day.loc[len(stays_per_day)] = new_row

        stays_per_day['DayOfWeek'] = pd.to_datetime(stays_per_day['Date']).dt.dayofweek
        stays_per_day['HoursPerDay'] = stays_per_day['DurationOfStay'] / 24
        
        stays_per_day_shifted = stays_per_day.shift(-1)
        stays_per_day_shifted.columns = ['Next_' + i for i in stays_per_day.columns]
        
        # Filter for consecutive days with the same location
        mask = (stays_per_day_shifted['Next_Date'] - stays_per_day['Date'] == timedelta(days=1))

        # Create new dataframe
        overnights_df = pd.DataFrame({
            'PriorDate': stays_per_day.loc[mask, 'Date'], 
            'NextDate': stays_per_day_shifted.loc[mask, 'Next_Date'], 
            'PriorLastHit': stays_per_day.loc[mask, 'LastHit'], 
            'NextFirstHit': stays_per_day_shifted.loc[mask, 'Next_FirstHit'], 
            'Location': stays_per_day.loc[mask, 'Location'], 
            'Overnight': (stays_per_day_shifted['Next_Date'] - stays_per_day['Date'] == timedelta(days=1)) & (stays_per_day['Location'] == stays_per_day_shifted['Next_Location']) }) # & (stays_per_day['Location'] != 'Other')

        # Reset index
        overnights_df.dropna(inplace=True)
        overnights_df.reset_index(drop=True, inplace=True)
        
        stays_per_day['Overnight'] = False

        # Iterate over rows in overnights_df
        for _, row in overnights_df.iterrows():
            condition = (stays_per_day['Date'] == row['PriorDate']) & (stays_per_day['Location'] == row['Location']) & (stays_per_day['LastHit'] == row['PriorLastHit'])
            stays_per_day.loc[condition, 'Overnight'] = True
            
            condition = (stays_per_day['Date'] == row['NextDate']) & (stays_per_day['Location'] == row['Location']) & (stays_per_day['FirstHit'] == row['NextFirstHit'])
            stays_per_day.loc[condition, 'Overnight'] = True
            
        # Define the time to compare (12 AM)
        midnight = datetime.strptime('00:00:00', '%H:%M:%S').time()

        # Function to calculate the duration and update 'DurationOfStay'
        def update_duration(row):
            if row['Overnight']:
                if row['LastHit'] == stays_per_day[stays_per_day['Date'] == row['Date']]['LastHit'].max():
                    # Calculate the difference between 12 AM and the last hit
                    duration = (datetime.combine(row['Date'], midnight) - row['LastHit']).seconds / 3600
                    row['DurationOfStay'] += duration
                elif row['FirstHit'] == stays_per_day[stays_per_day['Date'] == row['Date']]['FirstHit'].min():
                    # Calculate the difference between 12 AM and the first hit
                    duration = (row['FirstHit'] - datetime.combine(row['Date'], midnight)).seconds / 3600
                    row['DurationOfStay'] += duration 
            return row

        # Apply the function to each row in the DataFrame
        stays_per_day = stays_per_day.apply(update_duration, axis=1)
        
        # Group by DeviceID, Date, and Location, sum the duration of stay
        duration_per_location = stays_per_day.groupby(['DeviceID', 'Date', 'Location'])['DurationOfStay'].sum().reset_index()

        # Calculate the remaining time for each day
        remaining_time_per_day = 24 - duration_per_location.groupby(['DeviceID', 'Date'])['DurationOfStay'].transform('sum')

        # Create a new category 'Unknown' with the remaining time
        unknown_location_duration_per_day = pd.DataFrame({'DeviceID': duration_per_location['DeviceID'], 'Date': duration_per_location['Date'], 'Location': 'Unknown', 'DurationOfStay': remaining_time_per_day})

        # Concatenate the original duration_per_location and the unknown_time_per_day
        location_duration_per_day = pd.concat([duration_per_location, unknown_location_duration_per_day], ignore_index=True)
        location_duration_per_day.drop_duplicates(inplace=True)
        location_duration_per_day.sort_values(by=['DeviceID', 'Date', 'Location'], ascending=True, inplace=True)

        # Return results
        return location_duration_per_day

    def duration_at_aoi_wrt_dow(self):
        """
        Stacked bar plot showing the duration spent at each location for each day of a week.
        """
        
        location_duration_per_day = self.get_location_duration_per_day()

        location_duration_per_day['DayOfWeek'] = pd.to_datetime(location_duration_per_day['Date']).dt.day_name()

        # Group by 'DayOfWeek' and 'Location' and sum the 'Duration'
        location_duration_per_dow = location_duration_per_day.groupby(['DayOfWeek', 'Location'])['DurationOfStay'].mean().unstack(fill_value=0)

        # Ensure all days of the week are present
        order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        location_duration_per_dow = location_duration_per_dow.reindex(order, fill_value=0)
        
        # Find the duration at the known locations
        known_locations_duration = location_duration_per_dow.drop(columns='Unknown').sum(axis=1)

        # Replace 'Unknown' with 24 where sum_other_columns is 0
        location_duration_per_dow['Unknown'] = location_duration_per_dow['Unknown'].where(known_locations_duration != 0, 24)

        # Reshape the DataFrame to long format for plotting
        duration_per_dow_long = location_duration_per_dow.reset_index().melt(id_vars='DayOfWeek', value_name='DurationOfStay', var_name='Location')
        duration_per_dow_long['Location'] = duration_per_dow_long['Location'].apply(lambda x: str(x))

        # My additions start here

        duration_per_dow_long = duration_per_dow_long[duration_per_dow_long['Location'] != 'Unknown']

        # Find the day with highest duration for each location
        max_duration_per_location = duration_per_dow_long.groupby(['Location', 'DayOfWeek'])['DurationOfStay'].max().reset_index()
        day_with_max_duration = max_duration_per_location.loc[max_duration_per_location.groupby('Location')['DurationOfStay'].idxmax()]

        # Aggregate total duration for each location per day of the week
        total_duration_per_day_location = duration_per_dow_long.groupby(['DayOfWeek', 'Location']).sum().reset_index()

        # Identify the location with the maximum total duration for each day of the week
        most_prominent_location_per_day = total_duration_per_day_location.loc[total_duration_per_day_location.groupby('DayOfWeek')['DurationOfStay'].idxmax()]

        # Remove rows where duration is 0
        most_prominent_location_per_day = most_prominent_location_per_day[most_prominent_location_per_day['DurationOfStay'] != 0]

        # Remove the function and the line after in case original data frame was in tuple format and already reduced
        def reduce_decimal(location_str, decimal_places=8):
            if location_str == 'Other' or 'Unknown' : return location_str
            parts = location_str.strip('()').split(',') # Convert each part to float, round it to desired decimal places, and convert back to string
            reduced_parts = [str(round(float(part), 8)) for part in parts]  # Convert string representation of tuple to a tuple
            reduced_location = '(' + ', '.join(reduced_parts) + ')' 
            return str(reduced_location)
        
        day_with_max_duration['Location'] = day_with_max_duration['Location'].apply(lambda x: reduce_decimal(x, decimal_places=8))
        most_prominent_location_per_day['Location'] = most_prominent_location_per_day['Location'].apply(lambda x: reduce_decimal(x, decimal_places=8))

        # Sort the DataFrame by day of the week
        day_with_max_duration['DayOfWeek'] = pd.Categorical(day_with_max_duration['DayOfWeek'], categories=order, ordered=True)
        day_with_max_duration = day_with_max_duration.sort_values(by='DayOfWeek').reset_index(drop=True)
        
        most_prominent_location_per_day['DayOfWeek'] = pd.Categorical(most_prominent_location_per_day['DayOfWeek'], categories=order, ordered=True)
        most_prominent_location_per_day = most_prominent_location_per_day.sort_values(by='DayOfWeek').reset_index(drop=True)

        day_description = []
        for _, row in day_with_max_duration.iterrows():
            if row['Location'] is not 'Other': 
                day_description_text = f"The location {row['Location']} is mostly visited on {row['DayOfWeek']} with a duration of {round(row['DurationOfStay'])} hours and {round(row['DurationOfStay'] % 1)} minutes."
                day_description_text = day_description_text
                day_description.append(day_description_text)

        prominence_description = []
        for _, row in most_prominent_location_per_day.iterrows():
            if row['Location'] == 'Other':
                prominence_description_text = f"On {row['DayOfWeek']} the user seems to have high movement activity."
                prominence_description_text = prominence_description_text
                prominence_description.append(prominence_description_text)
            else:
                prominence_description_text = f"On {row['DayOfWeek']} the location {row['Location']} is mostly visited with a duration of {round(row['DurationOfStay'])} hours and {round(row['DurationOfStay'] % 1)} minutes."
                prominence_description_text = prominence_description_text
                prominence_description.append(prominence_description_text)

        duration_description = day_description + prominence_description

        # Update Location column based on the mapping dictionary
        duration_per_dow_long = self.ensure_location_mapping(duration_per_dow_long)

        # Plot Duration per day of week as a stacked bar chart
        fig = px.bar(duration_per_dow_long,
                    x='DayOfWeek', y='DurationOfStay', color='Address',
                    title='Duration at AOI per Day of Week',
                    labels={'DayOfWeek': 'Day of Week', 'Duration': 'Duration (hours)'},
                    category_orders={'DayOfWeek': order},
                    barmode='stack',  
                    color_discrete_sequence=self.dow_color_palette)

        # Add text inside bars
        fig.update_traces(texttemplate='%{y:.2f}', textposition='inside', textfont=dict(color='white'),
                            marker_line_color='white', marker_line_width=1.5)

        # Customize layout
        fig.update_layout(
            self.figure_layout,
            title=dict(text='Duration at Location per Day of Week', font=self.title_font),
            xaxis=dict(title='Day of Week', titlefont=self.axes_font, tickfont=self.axes_font),
            yaxis=dict(title='Duration (hours)', titlefont=self.axes_font, tickfont=self.axes_font)
        )

        duration_description = '\n'.join(duration_description)

        # Create a dictionary for the figure block
        figure_block = {
            'BLOCK_CONTENT': fig,
            'BLOCK_NAME': 'Location Duration Per Day of Week',
            'BLOCK_TYPE': 'PLOTLY',
            'BLOCK_ASSOCIATION': 'AOI',
            'DEVICE_ID': self.geolocation_data.device_id_geo
        }
        
        # Create a dictionary for the description block
        description_block = {
            'BLOCK_CONTENT': duration_description,
            'BLOCK_NAME': 'Location Duration Per Day of Week Description',
            'BLOCK_TYPE': 'TEXT',
            'BLOCK_ASSOCIATION': 'AOI',
            'DEVICE_ID': self.geolocation_data.device_id_geo
        }

        return figure_block, description_block

    # def get_color_for_percentage(self, color_range, percentage):
    #     for entry in color_range:
    #         if percentage >= entry['range'][0] and percentage <= entry['range'][1]:
    #             return entry['color']

    # def confidence_score(self):
    #     confidence_score = self.geolocation_data.aoi_confidence_results['Adjusted_Consistency_Score'].values[0]

    #     color_range = [
    #         {'range': [0, 10], 'color': '#FF0000'},   # Red
    #         {'range': [10, 20], 'color': '#FF4500'},  # Red to Orange-Red gradient
    #         {'range': [20, 30], 'color': '#FF8C00'},  # Orange
    #         {'range': [30, 40], 'color': '#FFD700'},  # Orange to Yellow gradient
    #         {'range': [40, 50], 'color': '#D5A760'},  # Yellow
    #         {'range': [50, 60], 'color': '#ADFF2F'},  # Yellow to Green-Yellow gradient
    #         {'range': [60, 70], 'color': '#32CD32'},  # Lime Green
    #         {'range': [70, 80], 'color': '#008000'},  # Green
    #         {'range': [80, 90], 'color': '#006400'},  # Dark Green
    #         {'range': [90, 100], 'color': '#000080'}  # Dark Blue
    #     ]

    #     # Determine the color based on the value
    #     font_color = None
    #     for range_ in color_range:
    #         if range_['range'][0] <= confidence_score * 100 <= range_['range'][1]:
    #             font_color = range_['color']
    #             break

    #     color = self.get_color_for_percentage(color_range, confidence_score * 100)

    #     gauge_chart = go.Figure(go.Indicator(
    #         mode="gauge+number",
    #         value=confidence_score * 100,
    #         domain={'x': [0, 1], 'y': [0, 1]},
    #         title={'text': "Confidence Score", 'font': {'size': self.title_font['size'], 'color': self.title_font['color'], 'family': self.title_font['family']}},
    #         gauge={
    #             'axis': {'range': [0, 100], 'tickmode': 'linear', 'tick0': 0, 'dtick': 10, 'tickfont': {'size': self.axes_font['size'], 'color': self.axes_font['color'], 'family': self.axes_font['family']}},
    #             'bar': {'color': color},
    #             'steps': [{
    #                 'range': [0, confidence_score * 100],
    #                 'color': color
    #             }],
    #         },
    #     ))

    #     # Set the font color based on the determined color
    #     gauge_chart.update_layout(font={'color': font_color, 'family': self.title_font['family']}, 
    #                                 paper_bgcolor="white", height=300)

    #     return gauge_chart

    def aoi_dataframe(self):
        data = self.geolocation_data.result_AOI.copy(deep=True)

        data.drop(columns=['index', 'COORDS'], inplace=True)

        data.rename(columns = {'NAME_ID': 'DEVICE ID', 
                            'LAT': 'LATITUDE', 
                            'LNG': 'LONGITUDE',
                            'NBR_HITS': 'TOTAL HITS', 
                            'Percentage': 'HITS PERCENTAGE',
                            'NBR_DAYS': 'TOTAL DAYS', 
                            'TYPE': 'AOI TYPE'}, inplace=True)

        data = data[['DEVICE ID', 'LATITUDE', 'LONGITUDE', 'AOI TYPE', 'TOTAL HITS', 'TOTAL DAYS', 'HITS PERCENTAGE', 'LOCATION']]

        data['AOI TYPE'] = data['AOI TYPE'].replace('', 'Undetermined')

        self.prompt_properties.prompt["add_ons_home_aoi"]["user"] = data.to_string(index=False)

        return data

    def suspiciousness_result(self):
        data = self.geolocation_data.df_polygon

        data.drop(columns=['INDEX_NUMBER', 'FLAGGED_AREA_CONTRIBUTION'], inplace=True)

        data.rename(columns = {
            'DEVICE_ID_GEO': 'DEVICE ID', 
            'SHAPE_NAME': 'SHAPE NAME', 
            'SHAPE_ID': 'SHAPE ID',
            'SHAPE_TYPE': 'SHAPE TYPE', 
            'LOCATION_COORDS': 'LOCATION',
            'SUSPICIOUSNESS_PERCENTAGE': 'SUSPICION PERCENTAGE', 
            'AREA_SUSPICIOUSNESS': 'AREA SUSPICIOUSNESS', 
            'DEVICE_SUSPICIOUSNESS': 'DEVICE SUSPICIOUSNESS'}, inplace=True)

        data = data[['DEVICE ID', 'SHAPE NAME', 'SHAPE ID', 'SHAPE TYPE', 'LOCATION', 'SUSPICION PERCENTAGE', 'AREA SUSPICIOUSNESS', 'DEVICE SUSPICIOUSNESS']]

        if not data.empty:
            self.prompt_properties.prompt["suspiciousness_evaluation"]["user"] = data.to_string(index=False)

        return data
    
    def aoi_display(self):
        data = self.geolocation_data.result_AOI
        # Create a Folium map centered around the first point in your DataFrame
        m = folium.Map(location=[data['LAT'].iloc[0], data['LNG'].iloc[0]], zoom_start=14)

        # Add circles for each point in the DataFrame
        for _, row in data.iterrows():
            # Add a marker at the current latitude and longitude
            folium.Marker(
                location=[row['LAT'], row['LNG']],
            ).add_to(m)

            # Draw a circle around the marker with a static radius
            folium.CircleMarker(
                location=[row['LAT'], row['LNG']],
                radius=50,
                color='blue',
                fill=True,
                fill_color='blue',
                fill_opacity=0.3,
                tooltip=f"COORDINATES: ({row['LAT']}, {row['LNG']})<br>TYPE: {row['TYPE'] if row['TYPE'] else 'Undetermined'}<br>NUMBER OF HITS: {row['NBR_HITS']}"
            ).add_to(m)

        return m
    
    ############################################################################################################
    # Trajectory functions
    ############################################################################################################

    def get_geospatial_graph(self, current_location, next_location, padding=0.01):
        # Unpack the latitude and longitude from each location
        lat1, lon1 = current_location
        lat2, lon2 = next_location
        
        # Calculate the min and max latitudes and longitudes to form the bounding box
        min_lat = min(lat1, lat2) - padding
        max_lat = max(lat1, lat2) + padding
        min_lon = min(lon1, lon2) - padding
        max_lon = max(lon1, lon2) + padding
        
        # Get the street network within the bounding box
        G = ox.graph_from_bbox(max_lat, min_lat, max_lon, min_lon, network_type='drive')
        self.graph = G
        
        # Convert the graph to GeoDataFrames
        nodes, edges = ox.graph_to_gdfs(G)
        self.graph_nodes = nodes
        self.graph_edges = edges
        
        return G

    def find_nearest_graph_node(self, graph, point):
        # Find the nearest network node to the given coordinates
        nearest_node = ox.distance.nearest_nodes(graph, point[1], point[0])
        return nearest_node
        
    def convert_node_to_coords(self, graph, node):
        node_data = graph.nodes[node]
        location_latitude, location_longitude = node_data['y'], node_data['x']
        return (location_latitude, location_longitude)

    def find_best_path(self, node1, node2):
        best_path = nx.shortest_path(self.graph, source=node1, target=node2)
        return best_path

    def interpolate_points_on_path(self, graph, path, meters=50):
        interpolated_points = []

        for i in range(len(path) - 1):
            start_node, end_node = path[i], path[i + 1]
            start_lat, start_lon = graph.nodes[start_node]['y'], graph.nodes[start_node]['x']
            end_lat, end_lon = graph.nodes[end_node]['y'], graph.nodes[end_node]['x']
            
            # Calculate the distance and interpolate points
            distance = ox.distance.great_circle(start_lat, start_lon, end_lat, end_lon)
            num_points = int(distance / meters)
            
            for j in range(num_points + 1):
                fraction = j / num_points if num_points > 0 else 0
                interp_lat = start_lat + (end_lat - start_lat) * fraction
                interp_lon = start_lon + (end_lon - start_lon) * fraction
                interpolated_points.append((interp_lat, interp_lon))

        if len(interpolated_points) == 0:
            return interpolated_points
        else:
            if interpolated_points[-1] != (end_lat, end_lon):
                interpolated_points.append((end_lat, end_lon))
        
        return interpolated_points

    def create_predictive_map(self, interpolated_points, current_location):
        # create a Folium map
        m = folium.Map(location = current_location, zoom_start=15)

        # Convert your interpolated points into the format expected by TimestampedGeoJson
        features = []
        current_time = datetime.now()

        for i, (lat, lon) in enumerate(interpolated_points):
            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [lon, lat],
                },
                'properties': {
                    'times': [(current_time + timedelta(seconds=i*2)).isoformat()], 
                    'icon': 'circle',
                    'iconstyle': {
                        'color' : 'darkblue',
                        'fillColor': 'darkblue',
                        'fillOpacity': 0.5,
                        'stroke': 'false',
                        'radius': 10
                    },
                    'popup': f'Point {i}',
                }
            }
            features.append(feature)

        # Create a TimestampedGeoJson object and add it to your map
        TimestampedGeoJson({
                    'type': 'FeatureCollection',
                    'features': features,
                }, 
                period='PT2S', # Controls the appearance interval of markers; PT1H = every hour, adjust as needed
                add_last_point=True, # Whether to add a permanent marker at the end of the series
                auto_play=True, # Set to True if you want the animation to play automatically
                loop=True, # Set to True if you want the animation to loop
        ).add_to(m)

        return m

    def display_next_location_trajectory(self, current_location, next_location):
        current_location = ast.literal_eval(current_location)
        next_location = ast.literal_eval(next_location)

        graph = self.get_geospatial_graph(current_location, next_location)

        # Use functions to find the nearest graph nodes
        start_node = self.find_nearest_graph_node(graph, current_location)
        end_node = self.find_nearest_graph_node(graph, next_location)

        # Find the best path between these nodes
        best_path = self.find_best_path(start_node, end_node)

        # Create a list including some points from the best_path variable that are separated by 50 meters
        interpolated_points = self.interpolate_points_on_path(graph, best_path, 50)

        if len(interpolated_points) == 0:
            return "No Available next location"
        else:
            m = self.create_predictive_map(interpolated_points, current_location)

        return m
    
    def display_next_location_trajectory_antpath(self, current_location, next_location):
        current_location = ast.literal_eval(current_location)
        next_location = ast.literal_eval(next_location)

        graph = self.get_geospatial_graph(current_location, next_location)

        # Find the nearest graph nodes to these points
        start_node = self.find_nearest_graph_node(graph, current_location)
        end_node = self.find_nearest_graph_node(graph, next_location)

        # Compute the shortest path between these nodes
        best_path = self.find_best_path(start_node, end_node)

        # Convert the nodes in the path back to coordinates
        route_coords = [self.convert_node_to_coords(graph, node) for node in best_path]

        # Create a map centered around the start point
        m = folium.Map(location = current_location, zoom_start=15)

        # Use AntPath to plot the route
        AntPath(
            locations=route_coords,
            color='darkblue',  # Set the path color to dark blue
            delay=1000,  # Set the animation delay (in milliseconds)
            dash_array=[10, 20],  # Adjust the dash pattern (optional)
            pulseColor = "#DDDDDD",
            opacity = 0.8
        ).add_to(m)

        return m