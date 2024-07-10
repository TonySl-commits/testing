##############################################################################################################################
    # Imports
##############################################################################################################################

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np

from geopy.distance import geodesic
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from datetime import datetime, timedelta
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.aoi_classification.new_geolocation_data import GeoLocationData
from vcis.utils.utils import CDR_Properties
from vcis.utils.utils import CDR_Utils

class AOIReportFunctions():
    def __init__(self):
        self.geolocation_data : GeoLocationData
        self.cassandra_tools = CassandraTools()
        self.properties = CDR_Properties()
        self.utils = CDR_Utils()
        self.locations_df =  pd.DataFrame()
        self.aoi_dataframe_result = pd.DataFrame()
        self.dow_color_palette = ['#03045E', '#023E8A', '#0077B6', '#0096C7', '#00B4D8', 
                                  '#48CAE4', '#90E0EF', '#ADE8F4', '#CAF0F8']
        self.months_color_palette = ['#1130A7', '#125F92', '#128F7E', '#13BE69', '#13EE55', '#A4D82F', 
                                     '#ECCD1C', '#FC9C14', '#CB620F', '#9A270A', '#561463', '#1100BB']
        
        self.figure_layout = {'paper_bgcolor': 'rgba(255, 255, 255, 1)',
                                'plot_bgcolor': 'rgba(255, 255, 255, 1)'}
        
        self.title_font = dict(family="Bahnschrift SemiBold", size=20, color='#03045E')
        self.axes_font = dict(family="Bahnschrift SemiBold", size=15, color='#03045E')
        # self.current_time = datetime.now()
        self.current_time = datetime.strptime("2023-09-25 14:43:00", '%Y-%m-%d %H:%M:%S')
        self.order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        self.likely_location = None
        self.expected_duration = None
        self.potential_next_location = None
        self.location_confidence_score = None

##############################################################################################################################
    # Reporting Functions
##############################################################################################################################       
    
    def initialize_aoi_report(self, geolocation_data):
        self.geolocation_data = geolocation_data

    def get_locations_df(self):
        if not self.locations_df.empty:
            return self.locations_df
        else:
            data = self.geolocation_data.data
            no_outliers = self.geolocation_data.no_outliers

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
    
    def likely_location_per_dow_hod(self):
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
        self.likely_location = subset.iloc[:, 2:].idxmax(axis=1).values[0]
        
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

        self.likely_location_per_dow_hod()
        likely_location = self.likely_location

        # Check if system_time falls within any time interval for the specified location
        interval_mask = (final_df['Location'] == likely_location) & (final_df['start_time'].dt.time <= self.current_time.time()) & (final_df['end_time'].dt.time >= self.current_time.time())
        
        if interval_mask.any():
            
            # Calculate average duration for the interval
            self.expected_duration = final_df.loc[interval_mask, 'duration_minutes'].mean()
            return final_df
        
        # self.expected_duration = "Data Unretrievable"
        return final_df

    def data_prep(self):

        expected_durations_df = self.get_expected_duration_at_AOI_per_stay()
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

                if expected_durations_df.iloc[index + 1]['Location'] == 'Other':
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

        return itinerary_df

    def next_likely_location(self):
        # Your data frame
        data = self.data_prep()
        data = data[data['CurrentAOI'] == self.likely_location]
        data = data.drop(['CurrentAOI'], axis=1).reset_index(drop=True)
        df = data
        print(len(df))
        # df['NextAOI'] = df['NextAOI'].apply(lambda x: str(x))

        # Features and target variable
        X = df[['HourOfDay', 'Weekend', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']]
        y = df['NextAOI']

        # Split the data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Create the Random Forest model
        rf_classifier = RandomForestClassifier()

        rf_classifier.fit(X_train, y_train)
        y_pred = rf_classifier.predict(X_test)

        # Evaluate the accuracy of the best model
        accuracy = accuracy_score(y_test, y_pred)
        
        self.location_confidence_score = accuracy

        # Create an empty dictionary
        test = {}

        # Fill the dictionary with the current time information
        test['HourOfDay'] = self.current_time.hour
        test['Weekend'] = self.current_time.weekday() >= 5  # True if Saturday or Sunday, False otherwise

        # Fill in the specific days of the week
        for day in self.order:
            test[day] = day == self.current_time.strftime('%A')

        test_df = pd.DataFrame([test])
        y_pred = rf_classifier.predict(test_df)
        next_likely_location = y_pred[0]

        values = next_likely_location.strip('()').split(',')
        rounded_values = [f'{round(float(value), 8)}' for value in values]
        next_likely_location = f'({rounded_values[0]}, {rounded_values[1]})'

        self.potential_next_location = next_likely_location

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
            new_row = {'DeviceID': device_id_geo,
                       'Date': date,
                       'Location': current_location,
                       'DurationOfStay': stay_duration.total_seconds() / 3600,
                       'MinHour': stay_start_time.hour,
                       'MaxHour': stay_end_time.hour,
                       'FirstHit': stay_start_time,
                       'LastHit': stay_end_time}
            
            stays_per_day.loc[len(stays_per_day)] = new_row

        stays_per_day['DayOfWeek'] = pd.to_datetime(stays_per_day['Date']).dt.dayofweek
        stays_per_day['HoursPerDay'] = stays_per_day['DurationOfStay'] / 24
        
        stays_per_day_shifted = stays_per_day.shift(-1)
        stays_per_day_shifted.columns = ['Next_' + i for i in stays_per_day.columns]
        
        # Filter for consecutive days with the same location
        mask = (stays_per_day_shifted['Next_Date'] - stays_per_day['Date'] == timedelta(days=1))

        # Create new dataframe
        overnights_df = pd.DataFrame({'PriorDate': stays_per_day.loc[mask, 'Date'], 
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

        # Plot Duration per day of week as a stacked bar chart
        fig = px.bar(duration_per_dow_long,
                    x='DayOfWeek', y='DurationOfStay', color='Location',
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

        return fig
