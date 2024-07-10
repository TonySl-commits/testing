import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from IPython.display import display
from datetime import timedelta, datetime
import itertools
import time
import folium

from vcis.aoi_classification.aoi_support_functions import reverse_geocoding
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.utils.properties import CDR_Properties
from vcis.engine_break_exception import EngineBreakException

class GeoLocationData:
    def __init__(self, device_id_geo, data):
        # Data
        self.device_id_geo = device_id_geo
        self.data = data

        # Clustering Process
        self.no_outliers = None

        # Analysis Process
        self.dow_df = pd.DataFrame(index=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
        self.dow_list = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

        # Results
        self.result_AOI = None
        self.df_polygon = pd.DataFrame(columns=('INDEX_NUMBER','DEVICE_ID_GEO','SHAPE_NAME','SHAPE_ID','SHAPE_TYPE','LOCATION_COORDS','SUSPICIOUSNESS_PERCENTAGE','FLAGGED_AREA_CONTRIBUTION','AREA_SUSPICIOUSNESS','DEVICE_SUSPICIOUSNESS'))

        # Tools
        self.oracle_tools = OracleTools()
        self.properties = CDR_Properties()

    ##############################################################################################################
    ###     AOI Identification
    ##############################################################################################################

    def sample_geospatial_data(self, data):     
        # 1. Get the number of samples per hour of the day
        data['HourOfDay'] = data['Timestamp'].dt.hour
        samples_per_hour = data.groupby('HourOfDay').size()

        # 2. Sum the number of samples from 6 to 24 o'clock, and get the average
        avg_samples_per_hod = samples_per_hour.loc[6:23].sum() / 18
        avg_samples_per_hod = round(avg_samples_per_hod)

        print('INFO:    AVERAGE SAMPLES PER HOUR OF DAY', avg_samples_per_hod)

        # 3. Compute the number of recorded days
        data['Date'] = data['Timestamp'].dt.date
        recorded_days = data['Date'].nunique()

        # 4. Compute the number of samples based on the formula
        samples = avg_samples_per_hod * 3 * (3/7) * (recorded_days / 7)

        return samples
    
    def get_geo_sampling_list(self, data):
        # Get the number of samples 
        samples = self.sample_geospatial_data(data)

        # Get the number of steps in the geospatial sampling list
        num_steps = int(np.ceil((samples - 2) / (1/3)))

        # Construct geospatial sampling list
        sampling_list = samples * (1 - (1/3)) ** np.arange(num_steps + 1)

        # Round number of samples in the sampling list
        sampling_list = np.rint(sampling_list)

        # Filter sampling list to have minimum number of samples greater than or equal to 10
        sampling_list = sampling_list[sampling_list >= 10]
        print('INFO:    GEOSPATIAL SAMPLING LIST:', sampling_list)

        return sampling_list
    
    def daily_sample_geospatial_data(self, data):
        # Convert 'Timestamp' column to datetime if it's not already in datetime format
        data['Timestamp'] = pd.to_datetime(data['Timestamp'])

        # Filter data from hours 6 to 23
        filtered_data = data[(data['Timestamp'].dt.hour >= 6) & (data['Timestamp'].dt.hour <= 23)]

        # Group by hour and count samples
        samples_per_hour = filtered_data.groupby(filtered_data['Timestamp'].dt.hour).size()

        average_count_per_hour = samples_per_hour.mean()

        return average_count_per_hour

    def get_geo_daily_sampling_list(self, data):
        samples = self.daily_sample_geospatial_data(data)

        # Get the number of steps in the geospatial sampling list
        num_steps = int(np.ceil((samples - 2) / (1/3)))

        # Construct geospatial sampling list
        sampling_list = samples * (1 - (1/3)) ** np.arange(num_steps + 1)

        # Round number of samples in the sampling list
        sampling_list = np.rint(sampling_list)

        # Filter sampling list to have minimum number of samples greater than or equal to 10
        sampling_list = sampling_list[sampling_list >= 5]
        print('INFO:    GEOSPATIAL SAMPLING LIST:', sampling_list)

        return sampling_list

    def day_by_day_cluster_analysis(self, data):
        data['Timestamp'] = pd.to_datetime(data['Timestamp'], unit='ms')

        # Initialize an empty DataFrame to store the results
        final_results = pd.DataFrame(columns=data.columns)

        # Iterate over each day's data
        for date, daily_data in data.groupby(data['Timestamp'].dt.date):
            print(daily_data)
            # Skip if there's only one sample for the day
            if len(daily_data) <= 1:
                continue
            
            # Get the number of samples 
            sampling_list = self.get_geo_daily_sampling_list(daily_data)
            eps_linspace = np.linspace(0.3, 1, 6)
            print(f"eps_linspace = {eps_linspace}")

            if sampling_list.size == 0:   
                raise EngineBreakException("Not enough samples to analyze clusters!")   

            # While loop instead of a for loop for min_samples
            index = 0
            min_samples_end_not_reached = True
            clusters_not_found = True
            EPS = 0.3

            while clusters_not_found and min_samples_end_not_reached:
                min_samples = int(sampling_list[index])
                clustering = DBSCAN(eps=EPS / 6371, min_samples=min_samples, algorithm='ball_tree', metric='haversine')
                clustering.fit(daily_data[['Latitude', 'Longitude']])
                daily_data['Cluster'] = clustering.labels_

                no_outliers = daily_data[daily_data['Cluster'] != -1]

                clusters_not_found = True if no_outliers.empty else False
                min_samples_end_not_reached = True if index != len(sampling_list) - 1 else False
                index += 1

            print('INFO:    MIN SAMPLES SELECTED', min_samples)

            # While loop for eps values if clusters are not found with min_samples
            index = 0  
            eps_linspace_end_not_reached = True
            clusters_not_found = True

            while clusters_not_found and eps_linspace_end_not_reached:
                EPS = (eps_linspace[index])
                print(f"eps = {EPS}")

                clustering = DBSCAN(eps=EPS / 6371., min_samples=10, algorithm='ball_tree', metric='haversine')
                clustering.fit(daily_data[['Latitude', 'Longitude']])
                daily_data['Cluster'] = clustering.labels_

                no_outliers = daily_data[daily_data['Cluster'] != -1]

                clusters_not_found = True if no_outliers.empty else False
                eps_linspace_end_not_reached = True if index != len(eps_linspace) - 1 else False
                index += 1

            print('INFO:    EPSILON', EPS)

            if not no_outliers.empty:
                final_results = final_results.append(no_outliers, ignore_index=True)

        print('INFO:    CLUSTERS FOUND',  no_outliers['Cluster'].nunique())

        # Get centroids for each Cluster
        final_results = self.get_cluster_centroids(final_results)

        # Store results in class variables if needed
        self.data = final_results
        self.no_outliers = final_results[final_results['Cluster'] != -1]

        print("Day by day clustering is done")

        return self.data, self.no_outliers


    def default_cluster_analysis(self, data):
        data['Timestamp'] = pd.to_datetime(data['Timestamp'], unit='ms')

        start_time = data['Timestamp'].min()
        end_time = data['Timestamp'].max()

        if start_time == end_time:
            raise EngineBreakException("Not enough samples to analyze clusters (one sample found)!")
        
        # Get the number of samples 
        sampling_list = self.get_geo_sampling_list(data)
        eps_linspace = np.linspace(0.3, 1, 6)

        if sampling_list.size == 0:   
            raise EngineBreakException("Not enough samples to analyze clusters!")   

        # While loop instead of a for loop
        index = 0
        min_samples_end_not_reached = True
        eps_linspace_end_not_reached = True
        clusters_not_found = True
        EPS = 0.3

        while clusters_not_found and min_samples_end_not_reached:
            min_samples = int(sampling_list[index])

            clustering = DBSCAN(eps=EPS / 6371., min_samples=min_samples, algorithm='ball_tree', metric='haversine')
            clustering.fit(data[['Latitude', 'Longitude']])
            data['Cluster'] = clustering.labels_

            no_outliers = data[data['Cluster'] != -1]

            clusters_not_found = True if no_outliers.empty else False
            min_samples_end_not_reached = True if index != len(sampling_list) - 1 else False
            index += 1

        print('INFO:    MIN SAMPLES SELECTED', min_samples)

        index = 0  
        while clusters_not_found and eps_linspace_end_not_reached:
            EPS = int(eps_linspace[index])

            clustering = DBSCAN(eps=EPS / 6371., min_samples=10, algorithm='ball_tree', metric='haversine')
            clustering.fit(data[['Latitude', 'Longitude']])
            data['Cluster'] = clustering.labels_

            no_outliers = data[data['Cluster'] != -1]

            clusters_not_found = True if no_outliers.empty else False
            eps_linspace_end_not_reached = True if index != len(eps_linspace) - 1 else False
            index += 1

        # Measure tightness of clusters
        print('INFO:    EPSILON', EPS)
        
        # Get centroids for each Cluster
        data = self.get_cluster_centroids(data)
        no_outliers = data[data['Cluster'] != -1]

        data.drop(columns=['index'], inplace=True)
        no_outliers.drop(columns=['index'], inplace=True)

        display('NO OUTLIERS:    ', no_outliers)
        display('NO OUTLIERS:    ', no_outliers.columns)

        print('INFO:    CLUSTERS FOUND',  no_outliers['Cluster'].nunique())

        self.data = data
        self.no_outliers = no_outliers

        return data, no_outliers
    
    def display_geospatial_data_with_aois(self, data):
        # Plot the geospatial data on a folium map
        m = folium.Map(location=[data['Latitude'].mean(), data['Longitude'].mean()], zoom_start=12)

        # Display points using Circle Markers, blue color with opacity 0.5 and radius 3
        for _, row in data.iterrows():
            folium.CircleMarker(
                location=[row['Latitude'], row['Longitude']],
                radius=5,
                color='blue',
                fill_opacity=0.7
            ).add_to(m)

        # Display AOI Locations using Markers, red color 
        no_outliers = data[data['Cluster'] != -1]

        for location in no_outliers['Cluster'].unique():
            folium.Marker(
                location=[no_outliers[no_outliers['Cluster'] == location]['Latitude'].mean(), no_outliers[no_outliers['Cluster'] == location]['Longitude'].mean()],
                popup=folium.Popup(location, parse_html=True),
                icon=folium.Icon(color='red')
            ).add_to(m)

        # Save map to HTML file
        m.save("geo_data_map.html")

    def measure_compactness_of_clusters(self):
        pass

    def analyze_clusters(self, data, analysis_mode = 'default'):
        
        # Analyze Clusters
        if analysis_mode == 'default':
            print('INFO:    Analysis Process Started!')
            data, no_outliers = self.default_cluster_analysis(data)
        else:
            print('INFO:    Day By Day Analysis Process Started!')
            data, no_outliers = self.day_by_day_cluster_analysis(data)

        return data, no_outliers

    def get_cluster_centroids(self, data):
        data['Location'] = data.groupby('Cluster')[['Latitude', 'Longitude']].transform('mean').round(8).apply(tuple, axis=1)

        return data 

    def get_aoi_coordinate_list(self):
        # Get the DataFrame without outliers
        no_outliers = self.no_outliers
        
        # Convert timestamp to milliseconds and extract date from timestamp
        no_outliers['Timestamp'] = (no_outliers['Timestamp'].astype(int) // 10**6).astype(int)
        
        # Group by location and aggregate required columns into lists
        grouped_df = self.no_outliers.groupby('Location').agg({
            'Device_ID_geo': lambda x: x.tolist(),
            'Latitude': lambda x: x.tolist(),
            'Longitude': lambda x: x.tolist(),
            'Location_Name': lambda x: x.tolist(),
            'Service_Provider_ID': lambda x: x.tolist(),
            'Timestamp': lambda x: x.tolist()}).reset_index()
        
        # Merge all required columns into a single list
        grouped_df['COORDS'] = grouped_df.apply(lambda row: [[lat, lon, device_id, timestamp, location_name, service_provider_id]
                                                                for lat, lon, device_id, timestamp, location_name, service_provider_id
                                                                in zip(row['Latitude'], row['Longitude'], row['Device_ID_geo'], row['Timestamp'], row['Location_Name'], row['Service_Provider_ID'])], axis=1)
        
        # Drop unnecessary columns
        grouped_df = grouped_df[['Location', 'COORDS']]
        
        # Convert COORDS to string and reset index
        grouped_df['COORDS'] = grouped_df['COORDS'].astype(str).str.replace(r'\s', '', regex=True)
        
        return grouped_df


    def determine_AOIs(self, data, no_outliers):
        # Extract date from timestamp
        no_outliers['Date'] = no_outliers['Timestamp'].dt.date

        # Group by location and aggregate required columns into lists
        result_df = self.get_aoi_coordinate_list()
        
        # Calculate total hits per location
        total_hits = no_outliers.groupby('Location').size().reset_index(name='TOTAL_HITS')
        
        # Get total days per location
        total_days = no_outliers.groupby('Location')['Date'].nunique().reset_index(name='TOTAL_DAYS')
        
        # Merge total hits and total days with grouped dataframe
        result_df = pd.merge(result_df, total_hits, on='Location')
        result_df = pd.merge(result_df, total_days, on='Location')
        
        # Calculate percentage of hits per location from total hits
        result_df['PERCENTAGE_OF_HITS'] = (result_df['TOTAL_HITS'] / data.shape[0]) * 100
        result_df['PERCENTAGE_OF_HITS'] = result_df['PERCENTAGE_OF_HITS'].round(2)
        
        # Get address using reverse geocoding
        result_df['ADDRESS'] = result_df['Location'].apply(reverse_geocoding).tolist()

        # Get the Latitude and Longitude from each Location
        result_df[['LATITUDE', 'LONGITUDE']] = result_df['Location'].apply(lambda x: pd.Series([x[1], x[0]]))

        # Initialize AOI TYPE column 
        result_df['TYPE'] = ''
        
        # Sort by percentage of hits and reset index without dropping it
        result_df = result_df.sort_values(by='PERCENTAGE_OF_HITS', ascending=False).reset_index(drop=True)

        # Get the device id 
        result_df['DEVICE_ID_GEO'] = self.device_id_geo

        # Convert 'Timestamp' to datetime
        no_outliers['Timestamp'] = pd.to_datetime(no_outliers['Timestamp'], unit='ms')

        # Filter AOIs in which the device doesn't visit on many days or many hits
        total_days = no_outliers['Timestamp'].dt.date.nunique()
        total_hits = int(data.shape[0])

        result_df = result_df[(result_df['TOTAL_DAYS'] >= 0.1 * total_days) | (result_df['TOTAL_HITS'] >= 0.03 * total_hits)]
        
        # Reindex columns for clarity
        result_df = result_df[['DEVICE_ID_GEO', 'LATITUDE', 'LONGITUDE', 'TYPE', 'TOTAL_HITS', 'TOTAL_DAYS', 'PERCENTAGE_OF_HITS', 'ADDRESS', 'COORDS']]
        result_df.reset_index(inplace=True)

        print('INFO:    AOI DATAFRAME\n', result_df)
        
        return result_df
    
    ##############################################################################################################
    ###     Identifying Locations Visited Per Day, Duration Per Visit, and more...
    ##############################################################################################################

    def get_locations_df(self, data):
        locations_df = data.copy()

        # Set locations column to string type
        locations_df['Location'] = locations_df['Location'].astype(str)

        # Set Location to 'Other' where Cluster is -1
        locations_df.loc[locations_df['Cluster'] == -1, 'Location'] = 'Other'

        # Convert 'Timestamp' to datetime
        locations_df['Timestamp'] = pd.to_datetime(locations_df['Timestamp'])

        # Extract day of week, day of month and hour of day
        locations_df['DayOfWeek'] = locations_df['Timestamp'].dt.day_name()
        locations_df['DayOfMonth'] = locations_df['Timestamp'].dt.day
        locations_df['HourOfDay'] = locations_df['Timestamp'].dt.hour
        locations_df['Date'] = locations_df['Timestamp'].dt.date

        # Sort the data by DEVICE_ID_GEO and Timestamp
        locations_df.sort_values(['Timestamp'], inplace=True)

        return locations_df

    def get_location_duration_per_day(self, locations_df):
        # Check out how much time this code is taking to run
        start_time = time.time()
        device_id_geo = self.device_id_geo
        
        # Initialize the output dataframe
        location_stays_per_day = pd.DataFrame(columns=['DeviceID', 'Date', 'Location', 'DurationOfStay', 'StartHour', 'EndHour', 'FirstHit', 'LastHit'])

        # loop through each date for the current device ID
        for date in locations_df['Date'].unique():
            
            # Initialize variables to track the current location and stay information
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

                    location_stays_per_day.loc[len(location_stays_per_day)] = new_row

                    # Update the current location
                    current_location = row['Location']
                    stay_start_time = row['Timestamp']
                    stay_end_time = stay_start_time
                    stay_duration = timedelta(0)

                # Update the end time for the current stay
                stay_end_time = row['Timestamp']

            # Calculate the duration of the final stay
            stay_duration = stay_end_time - stay_start_time
            
            # Update the output dataframe
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
            
            location_stays_per_day.loc[len(location_stays_per_day)] = new_row

        # Get the Day of Week from 
        location_stays_per_day['DayOfWeek'] = pd.to_datetime(location_stays_per_day['Date']).dt.day_name()

        # Get the Hours spent at each Location per Day 
        location_stays_per_day['HoursPerDay'] = location_stays_per_day['DurationOfStay'] / 24

        print('LOCATION STAYS PER DAY -- V1:\n', location_stays_per_day)

        end_time = time.time()
        
        # print time taken in seconds
        print('TIME TAKEN IN SECONDS:n', end_time - start_time)

        return location_stays_per_day    

    def evaluate_overnight_stays(self, location_stays_per_day):

        # Get hour of day of first hit and last hit in location_stays_per_day
        location_stays_per_day['StartHour'] = pd.to_datetime(location_stays_per_day['FirstHit']).dt.hour
        location_stays_per_day['EndHour'] = pd.to_datetime(location_stays_per_day['LastHit']).dt.hour

        # Shift Location stays per day by 1
        location_stays_per_day_shifted = location_stays_per_day.shift(-1)
        
        # Fix column names
        location_stays_per_day_shifted.columns = ['Next_' + i for i in location_stays_per_day.columns]

        # Filter for consecutive days with the same location, and Prior Last Hit > 8 PM & Next First Hit < 8 AM
        mask = (location_stays_per_day_shifted['Next_Date'] - location_stays_per_day['Date'] == timedelta(days=1)) & (location_stays_per_day['EndHour'] >= 20) & (location_stays_per_day_shifted['Next_StartHour'] <= 8)

        # Create overnights dataframe
        overnights_df = pd.DataFrame({
            'PriorDate': location_stays_per_day.loc[mask, 'Date'], 
            'NextDate': location_stays_per_day_shifted.loc[mask, 'Next_Date'], 
            'PriorLastHit': location_stays_per_day.loc[mask, 'LastHit'], 
            'NextFirstHit': location_stays_per_day_shifted.loc[mask, 'Next_FirstHit'], 
            'Location': location_stays_per_day.loc[mask, 'Location'], 
            'Overnight': True
        }) 

        print('OVERNIGHTS:\n', overnights_df)

        # Drop NAs and reset index
        overnights_df.dropna(inplace=True)
        overnights_df.reset_index(drop=True, inplace=True)
        
        # Initialize Overnight to False
        location_stays_per_day['Overnight'] = False
        location_stays_per_day['Previous_Overnight'] = False

        # Check the overnights in overnights_df (if overnights_df is not empty), and set Overnight to True in location_stays_per_day where the condition is met
        if not overnights_df.empty:
            # Iterate through the rows in overnights_df
            for _, row in overnights_df.iterrows():
                # Set the 'Overnight' column to True in location_stays_per_day where the condition is met
                condition = (location_stays_per_day['LastHit'] == row['PriorLastHit']) & (location_stays_per_day['Location'] == row['Location'])
                location_stays_per_day.loc[condition, 'Overnight'] = True

                # Set the 'Previous_Overnight' column to True in location_stays_per_day where the condition is met
                condition = (location_stays_per_day['FirstHit'] == row['NextFirstHit']) & (location_stays_per_day['Location'] == row['Location'])
                location_stays_per_day.loc[condition, 'Previous_Overnight'] = True

        # Define the time to compare (12 AM)
        midnight = datetime.strptime('00:00:00', '%H:%M:%S').time()

        # Function to calculate the duration and update 'DurationOfStay'
        def update_duration(row):
            if row['Overnight']:
                
                if row['LastHit'] == location_stays_per_day[location_stays_per_day['Date'] == row['Date']]['LastHit'].max():
                    
                    # Calculate the difference between 12 AM and the last hit
                    duration = (datetime.combine(row['Date'], midnight) - row['LastHit']).seconds / 3600
                    row['DurationOfStay'] += duration 
                    
            if row['Previous_Overnight']:
                
                if row['FirstHit'] == location_stays_per_day[location_stays_per_day['Date'] == row['Date']]['FirstHit'].min():
                    
                    # Calculate the difference between 12 AM and the first hit
                    duration = (row['FirstHit'] - datetime.combine(row['Date'], midnight)).seconds / 3600
                    row['DurationOfStay'] += duration 

            return row

        # Apply the function to each row in the DataFrame
        stays_per_day = location_stays_per_day.apply(update_duration, axis=1)

        print('LOCATION VISITS PER DAY -- FINAL:\n', stays_per_day[['Date', 'Location', 'DurationOfStay', 'Overnight', 'Previous_Overnight', 'FirstHit', 'LastHit']])


        return stays_per_day

    def determine_unknown_durations_per_date(self, stays_per_day):   
        device_id_geo = self.device_id_geo
        
        # Group by DeviceID, Date, and Location, sum the duration of stay
        duration_per_location = stays_per_day.groupby(['DeviceID', 'Date', 'Location'])['DurationOfStay'].sum().reset_index()

        # Calculate the remaining time for each day
        remaining_time_per_day = 24.0 - duration_per_location.groupby(['DeviceID', 'Date'])['DurationOfStay'].transform('sum')

        # Create a new category 'Unknown' with the remaining time
        unknown_location_duration_per_day = pd.DataFrame({
            'DeviceID': device_id_geo, 
            'Date': duration_per_location['Date'], 
            'Location': 'Unknown', 
            'DurationOfStay': remaining_time_per_day})

        # Concatenate the original duration_per_location and the unknown_time_per_day
        location_duration_per_day = pd.concat([duration_per_location, unknown_location_duration_per_day], ignore_index=True)
        location_duration_per_day.drop_duplicates(inplace=True)
        location_duration_per_day.sort_values(by=['DeviceID', 'Date', 'Location'], ascending=True, inplace=True)
        # location_duration_per_day.reset_index(drop=True, inplace=True)

        # Round the Duration of Stay to 2 numbers after the decimal
        location_duration_per_day['DurationOfStay'] = location_duration_per_day['DurationOfStay'].round(2)

        print('LOCATION DURATION PER DAY -- FINAL:\n', location_duration_per_day)
        
        test = location_duration_per_day.groupby('Date')['DurationOfStay'].sum()

        print('DURATION OF STAY PER DAY -- FINAL:\n', test)

        # Return results
        return location_duration_per_day
    
    def identify_location_visits_per_day(self, data):

        locations_df = self.get_locations_df(data)

        location_stays_per_day = self.get_location_duration_per_day(locations_df)

        stays_per_day = self.evaluate_overnight_stays(location_stays_per_day) 

        location_duration_per_day = self.determine_unknown_durations_per_date(stays_per_day)

        return location_duration_per_day
    
    ##############################################################################################################
    ###     AOI Classification
    ##############################################################################################################

    def get_percentage_of_stays_per_location_per_dow(self, no_outliers):

        # Extract unique dates and their corresponding day of week
        no_outliers['DayOfWeek'] = no_outliers['Timestamp'].dt.day_name()
        no_outliers['Date'] = no_outliers['Timestamp'].dt.date

        unique_dates = no_outliers[['Date', 'DayOfWeek']].drop_duplicates()
        
        # Count occurrences of each day of the week
        day_of_week_count = unique_dates['DayOfWeek'].value_counts().sort_index()
        
        # Group data by location and day of week, count unique dates
        stays_per_location_per_dow = no_outliers.groupby(['Location', 'DayOfWeek'])['Date'].nunique().reset_index(name='Days')
        
        # Pivot the data to get counts per location per day of week
        stays_per_location_per_dow = stays_per_location_per_dow.pivot(index='DayOfWeek', columns='Location', values='Days').fillna(0)
        
        # Calculate percentage of stays per location per day of week
        stays_per_location_per_dow = stays_per_location_per_dow.div(day_of_week_count, axis=0) * 100
        
        # Reindex to include all days of the week
        dow_df = pd.DataFrame(index=self.dow_list)
        stays_per_location_per_dow = dow_df.merge(stays_per_location_per_dow, how='left', left_index=True, right_index=True).fillna(0)
        
        print('INFO:    stays_per_location_per_dow\n', stays_per_location_per_dow)

        return stays_per_location_per_dow, day_of_week_count


    def hits_per_location_per_dow(self, no_outliers):
        no_outliers['HourOfDay'] = no_outliers['Timestamp'].dt.hour

        # Bin the data into 6 intervals of 10 minutes each
        no_outliers['TimeBin'] = pd.cut(no_outliers['Timestamp'].dt.minute, bins=6, labels=False)

        # Count the number of bins with hits per location per day of week
        hits_per_date_per_location = no_outliers.groupby(['Date', 'DayOfWeek', 'Location', 'HourOfDay'])['TimeBin'].nunique()
        
        # Compute the average count of hits per day, hour of day
        hits_per_date_per_location = hits_per_date_per_location.groupby(['Date', 'DayOfWeek', 'Location', 'HourOfDay']).sum() / 6

        # Reset index
        hits_per_date_per_location = hits_per_date_per_location.reset_index()

        # Get the average consistency score per Date, Day of week and Location
        avg_hits_per_date_per_location = hits_per_date_per_location.groupby(['Date', 'DayOfWeek', 'Location'])[['TimeBin']].mean()
        
        # Reset index again
        avg_hits_per_date_per_location.reset_index(inplace=True)

        # Get the average consistency score per Day of week and Location
        avg_hits_per_location_per_dow = avg_hits_per_date_per_location.groupby(['DayOfWeek', 'Location'])[['TimeBin']].mean()
        
        # Reset index again
        avg_hits_per_location_per_dow.reset_index(inplace=True)

        # Get unique locations and day of week values
        locations = no_outliers['Location'].unique()

        # Generate all possible combinations of location and day of week
        location_dow_combos = list(itertools.product(self.dow_list, locations))

        # Create DataFrame with these combinations
        location_dow_df = pd.DataFrame(location_dow_combos, columns=['DayOfWeek', 'Location'])

        # Merge with avg_hits_per_date_per_location and reset index
        avg_hits_per_location_per_dow = location_dow_df.merge(avg_hits_per_location_per_dow, how='left', on=['DayOfWeek', 'Location']).fillna(0).reset_index(drop=True)

        # Round consistency score of hits wrt Day of Week and Location to 2 numbers after the decimal
        avg_hits_per_location_per_dow['TimeBin'] = avg_hits_per_location_per_dow['TimeBin'].round(2)

        # Create the pivot table
        avg_hits_per_location_per_dow = avg_hits_per_location_per_dow.pivot(index='DayOfWeek', columns='Location', values='TimeBin').fillna(0)
        avg_hits_per_location_per_dow = avg_hits_per_location_per_dow.reindex(self.dow_list)

        print(f'INFO:    avg_hits_per_location_per_dow\n', avg_hits_per_location_per_dow)

        return avg_hits_per_location_per_dow
        
    def determine_overnight_stays(self, stays_per_day, day_of_week_count):
        # Filter stays per day for days with overnight stays
        overnights_df = stays_per_day[stays_per_day['Overnight'] == True]

        # Count the number of overnight stays per location per day of week
        overnights_at_location = overnights_df.pivot_table(index='DayOfWeek', columns='Location', values='Overnight', aggfunc='size').fillna(0)  
        
        # Get the percentage of overnight stays per day of week per location
        overnights_at_location = overnights_at_location.div(day_of_week_count, axis=0) 

        # Reindex to include all days of the week
        dow_df = pd.DataFrame(index=self.dow_list)
        overnights_at_location = dow_df.merge(overnights_at_location, how='left', left_index=True, right_index=True).fillna(0)

        print('INFO:    overnights_at_location\n', overnights_at_location)

        return overnights_at_location 
    
    def get_number_of_days_spent_in_each_location(self, no_outliers):
        # Get the number of days spent in each location
        no_outliers['Date'] = no_outliers['Timestamp'].dt.date
        total_days = no_outliers['Date'].nunique()
        days_per_cluster = no_outliers.groupby('Location')['Date'].nunique()
        
        # Get the percentage of days spent at this location from total days recorded in the data
        days_at_location = days_per_cluster * 100 / total_days
        days_at_location = pd.DataFrame(days_at_location)
        days_at_location = days_at_location.rename(columns={'Date': 'DAYS_FROM_TOTAL'})

        return days_at_location 

    def home_aoi_identification(self, no_outliers, stays_per_location_per_dow, overnights_at_location):
        # Get the probability that an AOI is a Home from the device's stay at location wrt to Day Of Week
        home_weights = [0.1, 0.1, 0.1, 0.1, 0.2, 0.2, 0.2]
        home_weights_per_dow = pd.DataFrame({'DayOfWeek': self.dow_list, 'Weight': home_weights}).set_index('DayOfWeek')
        
        stays_per_location_per_dow = stays_per_location_per_dow.multiply(home_weights_per_dow['Weight'], axis=0).apply(sum)
        stays_per_location_per_dow = pd.DataFrame(stays_per_location_per_dow, columns=['DOW_FACTOR'])

        # Calculate total overnight stays at a location
        overnight_stays = overnights_at_location.sum()
        total_overnight_stays = overnights_at_location.sum().sum()

        # Calculate percentage of overnight stays in each location
        overnight_stays_per_location = (overnights_at_location / total_overnight_stays).sum()
        overnight_stays_per_location = pd.DataFrame(overnight_stays_per_location, columns=['OVERNIGHT_FROM_TOTAL_OVERNIGHTS'])
        
        # Get the percentage of days spent at this location from total days recorded in the data
        days_at_location = self.get_number_of_days_spent_in_each_location(no_outliers)

        overnights_per_period = overnight_stays / days_at_location
        overnights_per_period = pd.DataFrame(overnights_per_period, columns=['OVERNIGHTS_PER_PERIOD'])
        overnights_per_period.fillna(0, inplace=True)
        
        # Get the probability that a location is a Home from the number of overnight stays with respect to the total recorded days
        total_days = no_outliers['Date'].nunique()
        overnights_from_total = overnight_stays / (total_days)
        overnights_from_total = pd.DataFrame(overnights_from_total, columns=['OVERNIGHT_FROM_TOTAL_DAYS'])
        overnights_from_total.fillna(0, inplace=True)

        overnights_from_total = overnights_from_total.apply(lambda x: np.minimum(x, 1))
        
        home_likelihood = pd.merge(stays_per_location_per_dow, overnight_stays_per_location, left_index=True, right_index=True, how='outer') \
            .merge(days_at_location, left_index=True, right_index=True, how='outer') \
            .merge(overnights_from_total, left_index=True, right_index=True, how='outer') \
            .merge(overnights_per_period, left_index=True, right_index=True, how='outer')
        
        home_likelihood['OVERNIGHT_FACTOR'] = (home_likelihood['OVERNIGHTS_PER_PERIOD'] * 0.3) + \
                                                (home_likelihood['OVERNIGHT_FROM_TOTAL_DAYS'] * 0.4)  + \
                                                (home_likelihood['OVERNIGHT_FROM_TOTAL_OVERNIGHTS'] * 0.3) 
        
        home_likelihood['OVERNIGHT_IMPORTANCE'] = home_likelihood['OVERNIGHT_IMPORTANCE'].fillna(0) * 100

        overnight_columns = ['OVERNIGHT_FACTOR', 'OVERNIGHTS_PER_PERIOD', 'OVERNIGHT_FROM_TOTAL_DAYS', 'OVERNIGHT_FROM_TOTAL_OVERNIGHTS']
        
        home_likelihood[overnight_columns] = home_likelihood[overnight_columns].fillna(0) * 100
        
        home_likelihood['HOME_LIKELIHOOD'] = (home_likelihood['DOW_FACTOR'] * 0.15) + \
                                            (home_likelihood['OVERNIGHT_FACTOR'] * 0.7) + \
                                            (home_likelihood['DAYS_FROM_TOTAL'] * 0.15)
        
        home_likelihood['HOME_LIKELIHOOD'] = home_likelihood['HOME_LIKELIHOOD'].fillna(0)

        return home_likelihood
    
    ##############################################################################################################
    ###     Engine Launcher
    ##############################################################################################################
    
    # def aoi_analysis_and_classification(self, analysis_mode = 'default'):

    #     try:
    #         # Analyze Clusters
    #         self.analyze_clusters(analysis_mode)
            
    #         # Determine AOIs
    #         self.determine_AOIs()

    #         display('INFO:      AOI Detection Complete!')

    #         # AOI Classification
    #         display('INFO:      Home AOI Classification Complete!')

    #         display('INFO:  Work AOI Classification Complete!')

    #     except EngineBreakException as e:
    #         print("Engine Break Exception caught:   ", e)

    #     except Exception as ex:
    #         # Handle other exceptions
    #         print("ERROR:   ", ex) 

    def aoi_analysis_and_classification(self, analysis_mode = 'default'):
        data = self.data 

        # Analyze Clusters
        data, no_outliers = self.analyze_clusters(data, analysis_mode)

        self.display_geospatial_data_with_aois(data)

        # Determine AOIs
        aoi_result = self.determine_AOIs(data, no_outliers)

        display('INFO:      AOI Detection Complete!')

        locations_df = self.get_locations_df(data)

        location_stays_per_day = self.get_location_duration_per_day(locations_df)

        stays_per_day = self.evaluate_overnight_stays(location_stays_per_day) 

        location_duration_per_day = self.determine_unknown_durations_per_date(stays_per_day)

        stays_per_location_per_dow, day_of_week_count = self.get_percentage_of_stays_per_location_per_dow(no_outliers)

        hits_per_location_per_dow = self.hits_per_location_per_dow(no_outliers)

        overnights_at_location = self.determine_overnight_stays(stays_per_day, day_of_week_count)

        home_likelihood = self.home_aoi_identification(no_outliers, stays_per_location_per_dow, overnights_at_location)

        # AOI Classification
        display('INFO:      Home AOI Classification Complete!')

        display('INFO:      Work AOI Classification Complete!')


        
