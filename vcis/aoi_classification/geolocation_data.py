import pandas as pd
import numpy as np
from sklearn.cluster import KMeans, DBSCAN
from IPython.display import display
from datetime import timedelta
import math
import math

from vcis.aoi_classification.aoi_support_functions import filter_points, count_unique_dates, reverse_geocoding
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.utils.properties import CDR_Properties

class GeoLocationData:
    def __init__(self, device_id_geo, data):
        # Data
        self.device_id_geo = device_id_geo
        self.data = data

        # Clustering Process
        self.centroids_total = pd.DataFrame(columns=['Latitude', 'Longitude', 'Cluster'])
        self.no_outliers = None
        self.centroids_list = None
        self.custom_centroids = None
        self.AOIs_df = None

        # Analysis Process
        self.stays_per_day = None
        self.count_each_day_of_week = None
        self.per_of_stays_per_cluster_per_dow = None
        self.hits_per_cluster_per_dow = None
        self.overnights_df = None
        self.nbr_of_overnights_at_location = None
        self.perc_overnights_at_location = None
        self.home_weights_per_dow = None
        self.primary_home_cluster_centroid = None
        self.potential_secondary_home_cluster_centroid = None
        self.proba_is_home = None
        self.day_of_week_dict = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}

        # Metrics
        self.min_date = None
        self.max_date = None
        self.total_days = None
        self.unique_dates = None

        # Results
        self.result_AOI = None
        self.df_polygon = pd.DataFrame(columns=('INDEX_NUMBER','DEVICE_ID_GEO','SHAPE_NAME','SHAPE_ID','SHAPE_TYPE','LOCATION_COORDS','SUSPICIOUSNESS_PERCENTAGE','FLAGGED_AREA_CONTRIBUTION','AREA_SUSPICIOUSNESS','DEVICE_SUSPICIOUSNESS'))
        self.aoi_confidence_results = pd.DataFrame(columns=['Device_ID_geo', 'Min_Date', 'Max_Date', 'NUMBER_OF_RECORDED_DAYS', 'NUMBER_OF_RECORDED_HITS', 'Consistency_Score', 'Adjusted_Consistency_Score'])
        
        # Tools
        self.oracle_tools = OracleTools()
        self.properties = CDR_Properties()

    def analyze_clusters(self):
        # print(self.data)
        if len(self.data) > 1:
            min_date = self.data['Timestamp'].min()
            max_date = self.data['Timestamp'].max()
            freq_of_hits = (max_date - min_date) / len(self.data)
            freq_of_hits = round(freq_of_hits.total_seconds() / 60, 2)

            if min_date != max_date:
                n_samples = 60 / freq_of_hits
                num_steps = int(np.ceil((n_samples - 2) / 0.33))
                n_samples_list = n_samples * (1 - 0.33) ** np.arange(num_steps + 1)
                n_samples_list = np.rint(n_samples_list)
                n_samples_list = n_samples_list[n_samples_list >= 10]

                for n in n_samples_list:
                    if len(self.centroids_total) != 0:
                        continue

                    clustering = DBSCAN(eps=0.3 / 6371, min_samples=int(n), algorithm='ball_tree', metric='haversine')
                    clustering.fit(self.data[['Latitude', 'Longitude']])
                    self.data.loc[:, 'Cluster'] = clustering.labels_

                    self.no_outliers = self.data[['Device_ID_geo', 'Location_Name', 'Service_Provider_ID', 'Latitude', 'Longitude', 'Timestamp', 'Cluster']]
                    self.no_outliers = self.no_outliers[self.no_outliers['Cluster'] != -1]

                    for i in self.no_outliers['Cluster'].unique():
                        cluster_i = self.no_outliers[self.no_outliers['Cluster'] == i][['Latitude', 'Longitude', 'Cluster']]
                        centroids_list = cluster_i.mean(axis=0).to_list()
                        self.centroids_total.loc[len(self.centroids_total)] = centroids_list

                for eps in np.linspace(0.3, 1, 6):
                    if len(self.centroids_total) != 0:
                        continue

                    clustering = DBSCAN(eps=eps / 6371., min_samples=10, algorithm='ball_tree', metric='haversine')
                    clustering.fit(self.data[['Latitude', 'Longitude']])
                    self.data.loc[:, 'Cluster'] = clustering.labels_

                    self.no_outliers = self.data[['Device_ID_geo', 'Location_Name', 'Service_Provider_ID', 'Latitude', 'Longitude', 'Timestamp', 'Cluster']]
                    self.no_outliers = self.no_outliers[self.no_outliers['Cluster'] != -1]

                    for i in self.no_outliers['Cluster'].unique():
                        cluster_i = self.no_outliers[self.no_outliers['Cluster'] == i][['Latitude', 'Longitude', 'Cluster']]
                        centroids_list = cluster_i.mean(axis=0).to_list()
                        self.centroids_total.loc[len(self.centroids_total)] = centroids_list

                self.centroids_list = [(x[0], x[1]) for x in np.array(self.centroids_total)]
    
    
    def get_custom_centroids(self):
        if len(self.centroids_total) >=1:
            self.no_outliers.reset_index(inplace=True)
            self.no_outliers.drop(columns=['index', 'Cluster'], inplace=True) 
            X = self.no_outliers[['Latitude','Longitude']].values   

        elif len(self.centroids_total) == 0:  
            X = self.data[['Latitude','Longitude']].values
            self.no_outliers = self.data

        # Specify N
        if len(X) >= 3:
            if len(X) >= 30:
                N = range(2, 30)
            elif 3 <= len(X) < 30:
                N = range(2, len(X))

            k_before_filter = k_after_filter = 2 
            for k in N:
                if (k_before_filter == 2 and k_after_filter == 1):
                    centroids = new_centroids
                    break
                elif (k_before_filter > k_after_filter):
                    model = KMeans(n_clusters = k_after_filter)
                    model.fit(X)
                    centroids = model.cluster_centers_
                    centroids = [(x, y) for x, y in centroids]
                    break

                model = KMeans(n_clusters = k)
                model.fit(X)
                centroids = model.cluster_centers_
                centroids = [(x, y) for x, y in centroids]
                k_before_filter = len(centroids)
                new_centroids = centroids.copy()
                filter_points(new_centroids)
                k_after_filter = len(new_centroids) 

        if len(X) < 3:
            centroids_no_kmeans = []
            for A in X:
                center = (A[0], A[1])
                centroids_no_kmeans.append(center)
            centroids = centroids_no_kmeans
            filter_points(centroids) 

        self.custom_centroids = np.array(centroids)
        
        # calculate the Euclidean distance between each data point and the custom centroids
        distances = np.linalg.norm(X[:, np.newaxis] - self.custom_centroids, axis=2)

        # assign the label of the closest centroid to each data point
        cluster_labels = np.argmin(distances, axis=1)
        label_centroid_dict = {label: centroid for label, centroid in enumerate(self.custom_centroids)}

        # add the predicted cluster labels to the dataframe
        self.no_outliers['cluster_label'] = cluster_labels
        self.no_outliers['cluster_label_centroid'] = self.no_outliers['cluster_label'].map(label_centroid_dict).apply(tuple)
        
    def determine_AOIs(self):
        df = self.no_outliers.copy()
        df.drop(columns=['cluster_label'], inplace=True)
        df = df.reindex(columns=['Device_ID_geo', 'Location_Name', 'Service_Provider_ID', 'Timestamp', 'Latitude', 'Longitude', 'cluster_label_centroid'])
        df['Timestamp'] = df['Timestamp'].apply(lambda x: x.timestamp()).astype(int) // 10**6
        
        result_df = count_unique_dates(self.no_outliers, 'cluster_label_centroid')
        number_days = result_df['nbr_of_days'].tolist()
        
        address_centroids = []
       
        df_grouped = df.groupby('cluster_label_centroid').agg({"Latitude" : list, "Longitude" : list, 
                                                               "Device_ID_geo" : list, "Timestamp" : list, 
                                                               "Location_Name" : list, "Service_Provider_ID" : list})

        df_grouped['coords_list'] = df_grouped.apply(lambda x: [[lng, lat, device_id, timestamp, loc_name, spid] 
                                                                   for lat, lng, device_id, timestamp, loc_name, spid 
                                                                   in zip(x['Latitude'], x['Longitude'], x['Device_ID_geo'], x['Timestamp'], x['Location_Name'], x['Service_Provider_ID'])], axis=1)
        coords = df_grouped['coords_list'].tolist()

        df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms')
        
        AOIs_df = df.groupby(['Device_ID_geo','cluster_label_centroid']).size().reset_index(name='counts')
        total = AOIs_df['counts'].sum()
        AOIs_df['Percentage'] = AOIs_df['counts'] / total * 100
        
        for i in range(len(AOIs_df)):
            address_centroids.append(f"{reverse_geocoding(AOIs_df['cluster_label_centroid'][i])}")

        AOIs_df['LOCATION'] = address_centroids
        AOIs_df.insert(4, 'location_type', str(''), True)
        AOIs_df[['AOI_latitude', 'AOI_longitude']] = AOIs_df['cluster_label_centroid'].apply(lambda x: pd.Series([x[1], x[0]]))
        AOIs_df['NBR_DAYS'] = number_days
        AOIs_df['COORDS'] = coords
        AOIs_df['COORDS'] = AOIs_df['COORDS'].astype(str).str.replace(r'\s', '', regex=True)

        # Filter AOIs in which the device doesn't visit on many days or many hits
        total_nbr_of_days = self.no_outliers['Timestamp'].dt.date.nunique()
        total_nbr_of_hits = int(AOIs_df['counts'].sum())

        AOIs_df = AOIs_df[(AOIs_df['NBR_DAYS'] >= 0.15 * total_nbr_of_days) | (AOIs_df['counts'] >= 0.05 * total_nbr_of_hits)]

        total_nbr_of_hits = AOIs_df['counts'].sum()
        AOIs_df['Percentage'] = AOIs_df['counts'] / total_nbr_of_hits * 100
        AOIs_df['Percentage'] = AOIs_df['Percentage'].round(2) 

        AOIs_df = AOIs_df.sort_values(by='Percentage', ascending=False)
        AOIs_df.reset_index(inplace=True)
    
        AOIs_df.drop(columns=['cluster_label_centroid'], inplace=True)
        AOIs_df.columns = ['index','NAME_ID','NBR_HITS','Percentage', 'TYPE', 'LOCATION', 'LNG', 'LAT', 'NBR_DAYS','COORDS']
        self.AOIs_df = AOIs_df.reindex(columns=['index','NAME_ID', 'LAT','LNG','NBR_HITS','Percentage','NBR_DAYS','TYPE','LOCATION','COORDS'])
        
        AOIs = []

        aois_df = self.AOIs_df
        AOIs.append(aois_df)

        aois_df_copy = aois_df.copy()
        aois_df_copy.drop(columns=['COORDS'])
        
        result_AOI = pd.concat(AOIs)
        del result_AOI['index']
        result_AOI.reset_index(inplace=True)
        del result_AOI['index']
        result_AOI.reset_index(inplace=True)
        self.result_AOI = result_AOI

    def identify_all_stays_per_day(self):
        tmp = self.no_outliers[['cluster_label_centroid', 'Timestamp']]
        tmp['Device ID'] = self.device_id_geo
        tmp.columns = ['Location', 'Timestamp', 'Device ID']

        tmp['Date'] = tmp['Timestamp'].dt.date

        # sort the data by Device ID and Timestamp
        tmp.sort_values(['Device ID', 'Timestamp'], inplace=True)

        # initialize the output dataframe
        stays_per_day = pd.DataFrame(columns=['Device ID', 'Date', 'Location', 'Duration of Stay', 'Min Hour', 'Max Hour', 'Stay'])

        # loop through each date for the current device ID
        for date in tmp['Date'].unique():
            # initialize variables to track the current location and stay information
            current_location = tmp.loc[(tmp['Date'] == date), 'Location'].iloc[0]
            stay_start_time = tmp.loc[(tmp['Date'] == date), 'Timestamp'].iloc[0]
            stay_end_time = stay_start_time
            stay_duration = timedelta(0)
            stay_count = 1

            # loop through each row of data for the current device ID and date
            for i, row in tmp.loc[(tmp['Date'] == date)].iterrows():

                # Check if the location has changed
                if row['Location'] != current_location:

                    # Calculate the duration of the previous stay and update the output dataframe
                    stay_duration = stay_end_time - stay_start_time
                    new_row = {'Device ID': self.device_id_geo,
                            'Date': date,
                            'Location': current_location,
                            'Duration of Stay': stay_duration.total_seconds() / 3600,
                            'Min Hour': stay_start_time.hour,
                            'Max Hour': stay_end_time.hour,
                            'Stay': stay_count}

                    stays_per_day.loc[len(stays_per_day)] = new_row

                    # Update the current location and stay information
                    current_location = row['Location']
                    stay_start_time = row['Timestamp']
                    stay_end_time = stay_start_time
                    stay_duration = timedelta(0)
                    stay_count += 1

                # Update the end time for the current stay
                stay_end_time = row['Timestamp']

            # Calculate the duration of the final stay and update the output dataframe
            stay_duration = stay_end_time - stay_start_time
            new_row = {'Device ID': self.device_id_geo,
                        'Date': date,
                        'Location': current_location,
                        'Duration of Stay': stay_duration.total_seconds() / 3600,
                        'Min Hour': stay_start_time.hour,
                        'Max Hour': stay_end_time.hour,
                        'Stay': stay_count}
            
            stays_per_day.loc[len(stays_per_day)] = new_row

        stays_per_day['Day of week'] = pd.to_datetime(stays_per_day['Date']).dt.dayofweek
        stays_per_day['% Hours Spent in Stay Per Day'] = stays_per_day['Duration of Stay'] / 24
        self.stays_per_day = stays_per_day
        
    def get_per_of_stays_per_cluster_per_dow(self):
        self.min_date = self.no_outliers['Timestamp'].min()
        self.max_date = self.no_outliers['Timestamp'].max()
        
        unique_dates = pd.DataFrame(columns=['Date'])
        unique_dates['Date'] = self.no_outliers['Timestamp'].dt.date.drop_duplicates().reset_index(drop=True)
        unique_dates['Day of week'] = pd.to_datetime(unique_dates['Date']).dt.dayofweek
        self.unique_dates = unique_dates
        
        self.count_each_day_of_week = unique_dates['Day of week'].value_counts().sort_index()
        
        time_pattern_data = pd.DataFrame(columns=['Cluster', 'Day of week', 'Number of days'])
        for location in self.stays_per_day['Location'].unique():
            cluster = self.stays_per_day[self.stays_per_day['Location'] == location]
            for day in cluster['Day of week'].unique():
                cluster_day_of_week = self.stays_per_day[(self.stays_per_day['Location'] == location) & (self.stays_per_day['Day of week'] == day)].drop_duplicates(subset=["Date"])
                n_days = cluster_day_of_week['Day of week'].agg('count')
               
                time_pattern_data.loc[len(time_pattern_data)] = {'Cluster': location,
                                                                 'Day of week': day,
                                                                 'Number of days': n_days}

        time_pattern_data = time_pattern_data.sort_values(by=["Cluster", "Day of week", "Number of days"], ascending=[True, True, False])
        time_pattern_data.reset_index(drop=True, inplace=True)
        
        per_of_stays_per_cluster_per_dow = time_pattern_data.pivot(index='Day of week', columns='Cluster', values='Number of days').fillna(0)
        per_of_stays_per_cluster_per_dow = per_of_stays_per_cluster_per_dow.div(self.count_each_day_of_week, axis=0) * 100
        per_of_stays_per_cluster_per_dow.index = per_of_stays_per_cluster_per_dow.index.map(self.day_of_week_dict)
        
        # List of all days of the week
        all_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        all_days = pd.DataFrame(index=all_days)

        # Merge all_days with the original DataFrame
        per_of_stays_per_cluster_per_dow = all_days.merge(per_of_stays_per_cluster_per_dow, how='left', left_index=True, right_index=True) \
                                                    .fillna(0)
        
        self.per_of_stays_per_cluster_per_dow = per_of_stays_per_cluster_per_dow

    def get_nbr_of_hits_in_each_AOI_per_dow(self):
        tmp = self.no_outliers
        tmp['Day of week'] = pd.to_datetime(tmp['Timestamp']).dt.dayofweek
        hits_per_cluster_per_dow = tmp.pivot_table(index='Day of week', columns='cluster_label_centroid', aggfunc='size').fillna(0)

        hits_per_cluster_per_dow.index = hits_per_cluster_per_dow.index.map(self.day_of_week_dict)
        self.hits_per_cluster_per_dow = hits_per_cluster_per_dow
        
    def detect_overnight_stays_in_each_AOI(self):
        df = self.stays_per_day[['Date', 'Location', 'Duration of Stay', 'Min Hour','Max Hour', 'Stay', 'Day of week']]
        df_shifted = df.shift(-1)
        df_shifted.columns = ['Next ' + i for i in df.columns]

        # Filter for consecutive days with the same location
        mask = (df_shifted['Next Date'] - df['Date'] == timedelta(days=1))

        # Create new dataframe
        overnights_df = pd.DataFrame({'Prior Date': df.loc[mask, 'Date'], 'Next Date': df_shifted.loc[mask, 'Next Date'], 'Prior Last Hit': df.loc[mask, 'Max Hour'], 'Next First Hit': df_shifted.loc[mask, 'Next Min Hour'], 'Location': df.loc[mask, 'Location'], 'Overnight at Location': (df_shifted['Next Date'] - df['Date'] == timedelta(days=1)) & (df['Location'] == df_shifted['Next Location'])})

        # Reset index
        overnights_df.dropna(inplace=True)
        overnights_df.reset_index(drop=True, inplace=True)
        overnights_df = overnights_df[overnights_df['Overnight at Location'] == True]
        
        # Filter Prior Last Hit > 8 & Next First Hit < 9
        overnights_df = overnights_df[(overnights_df['Prior Last Hit'] >= 18) & (overnights_df['Next First Hit'] <= 9)]
        overnights_df['Day of week'] = pd.to_datetime(overnights_df['Prior Date']).dt.dayofweek
        self.overnights_df = overnights_df
        
    def get_locations_with_overnight_stays(self):
        self.nbr_of_overnights_at_location = self.overnights_df.pivot_table(index='Day of week', columns='Location', values='Overnight at Location', aggfunc='size').fillna(0)  
        self.perc_overnights_at_location = self.nbr_of_overnights_at_location.div(self.count_each_day_of_week, axis=0) * 100        
        
    def classify_AOI(self):
        dow = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        home_weights = [0.1, 0.1, 0.1, 0.1, 0.2, 0.2, 0.2]
        self.home_weights_per_dow = pd.DataFrame({'Day of week': dow, 'Weight': home_weights}).set_index('Day of week')
        
        proba_is_home_from_dow_data = self.per_of_stays_per_cluster_per_dow.multiply(self.home_weights_per_dow['Weight'], axis=0).apply(sum)
        proba_is_home_from_dow_data = pd.DataFrame(proba_is_home_from_dow_data, columns=['% Home from DOW'])

        # Calculate total overnight stays
        overnight_stays = self.nbr_of_overnights_at_location.sum()
        total_overnight_stays = self.nbr_of_overnights_at_location.sum().sum()

        # Calculate percentage of overnight stays in each location/cluster
        percent_overnight_stays = (self.nbr_of_overnights_at_location / total_overnight_stays).sum()
        percent_overnight_stays = pd.DataFrame(percent_overnight_stays, columns=['% Overnight Stays'])
        
        self.no_outliers['Date'] = self.no_outliers['Timestamp'].dt.date
        self.total_days = self.no_outliers['Date'].nunique()
        days_per_cluster = self.no_outliers.groupby('cluster_label_centroid')['Date'].nunique()
        
        percent_days_per_cluster = days_per_cluster * 100 / self.total_days
        percent_days_per_cluster = pd.DataFrame(percent_days_per_cluster)
        percent_days_per_cluster = percent_days_per_cluster.rename(columns={'Date': '% Days From Total'})

        overnights_per_period = overnight_stays / days_per_cluster
        overnights_per_period = pd.DataFrame(overnights_per_period, columns=['Overnights Per Period'])
        overnights_per_period.fillna(0, inplace=True)
        
        overnights_from_total = overnight_stays / (self.total_days)
        overnights_from_total = pd.DataFrame(overnights_from_total, columns=['Overnights From Total'])
        overnights_from_total.fillna(0, inplace=True)
        overnights_from_total = overnights_from_total.apply(lambda x: np.minimum(x, 1))
        
        proba_is_home = pd.merge(proba_is_home_from_dow_data, percent_overnight_stays, left_index=True, right_index=True, how='outer')            .merge(percent_days_per_cluster, left_index=True, right_index=True, how='outer')            .merge(overnights_from_total, left_index=True, right_index=True, how='outer')            .merge(overnights_per_period, left_index=True, right_index=True, how='outer')
        
        proba_is_home['Importance of Overnight Stays'] = (proba_is_home['Overnights Per Period'] * 0.3) + (proba_is_home['Overnights From Total'] * 0.4)  + (proba_is_home['% Overnight Stays'] * 0.3) 
        
        proba_is_home['Overnights Per Period'] = proba_is_home['Overnights Per Period'] * 100
        proba_is_home['Overnights Per Period'] = proba_is_home['Overnights Per Period'].fillna(0)

        proba_is_home['% Overnight Stays'] = proba_is_home['% Overnight Stays'] * 100
        proba_is_home['% Overnight Stays'] = proba_is_home['% Overnight Stays'].fillna(0)

        proba_is_home['Importance of Overnight Stays'] = proba_is_home['Importance of Overnight Stays'].fillna(0)
        proba_is_home['Importance of Overnight Stays'] *= 100

        
        proba_is_home['% Home'] = (proba_is_home['% Home from DOW'] * 0.15) + (proba_is_home['Importance of Overnight Stays'] * 0.7) + (proba_is_home['% Days From Total'] * 0.15)
        proba_is_home['% Home'] = proba_is_home['% Home'].fillna(0)

        self.proba_is_home = proba_is_home
        
    def find_home_and_potential_second_home(self):
        # Find the cluster label with the highest % Home
        max_home_cluster = self.proba_is_home['% Home'].idxmax()

        # Check if the % Home is less than 35% and set the primary_home_cluster_label accordingly
        if self.proba_is_home[self.proba_is_home['% Home'] > 35].shape[0] >= 2:
            self.primary_home_cluster_centroid = max_home_cluster
            self.potential_secondary_home_cluster_centroid = self.proba_is_home[self.proba_is_home['% Home'] > 35].sort_values(by='% Home', ascending=False).index[1]
        else:
            self.primary_home_cluster_centroid = max_home_cluster
            self.potential_secondary_home_cluster_centroid = None

        # Check if % Home is less than 35% and set primary_home_cluster_label accordingly
        if self.proba_is_home['% Home'].max() < 35:
            self.primary_home_cluster_centroid = None
            
        self.result_AOI['Centroid'] = [x for x in zip(self.result_AOI['LAT'], self.result_AOI['LNG'])]
        centroid_values = self.result_AOI['Centroid']

        # Update 'TYPE' column using tuple comparison
        self.result_AOI.loc[centroid_values == self.primary_home_cluster_centroid, 'TYPE'] = 'Home'
        self.result_AOI.loc[centroid_values == self.potential_secondary_home_cluster_centroid, 'TYPE'] = 'Potential Secondary Home'
        self.result_AOI.drop(columns=['Centroid'], inplace=True)
        
    def identify_home_AOIs(self):
        self.identify_all_stays_per_day()
        self.get_per_of_stays_per_cluster_per_dow()
        self.get_nbr_of_hits_in_each_AOI_per_dow()
        self.detect_overnight_stays_in_each_AOI()
        self.get_locations_with_overnight_stays()
        self.classify_AOI()
        self.find_home_and_potential_second_home()
        
    def filter_potential_work_locations(self):
        potential_workplace_locations = self.result_AOI[self.result_AOI['TYPE'] != 'Home']
        potential_workplace_locations = potential_workplace_locations[potential_workplace_locations['Percentage'] >= 5]
        return list(zip(potential_workplace_locations['LAT'], potential_workplace_locations['LNG']))
        
    def breakdown_of_time_per_location_per_dow(self, potential_work_locations):
        breakdown_of_time_per_location_per_dow = self.stays_per_day.groupby(['Location', 'Date', 'Day of week'])['Duration of Stay'].sum().reset_index()
        breakdown_of_time_per_location_per_dow = pd.pivot_table(breakdown_of_time_per_location_per_dow, values='Duration of Stay', index=['Date', 'Day of week'], columns=['Location'], fill_value=0)

        location_means = breakdown_of_time_per_location_per_dow.mean()
        sorted_columns = location_means.sort_values(ascending=False).index
        breakdown_of_time_per_location_per_dow = breakdown_of_time_per_location_per_dow[sorted_columns]

        # Keep important locations only
        breakdown_of_time_per_location_per_dow = breakdown_of_time_per_location_per_dow[potential_work_locations]
        return breakdown_of_time_per_location_per_dow

    def study_time_consistency(self, potential_work_locations):
        potential_work_aois = self.stays_per_day[self.stays_per_day['Location'].isin(potential_work_locations)]

        avg_time_per_hod = potential_work_aois.groupby(['Location', 'Day of week'])['Duration of Stay'].mean().reset_index()

        mean_std_df = avg_time_per_hod.reset_index().groupby('Location')['Duration of Stay'].agg(['mean', 'std'])
        mean_hours = mean_std_df['mean'].apply(round).fillna(0)
        std_dev_hours = mean_std_df['std'].fillna(np.inf)

        # Computing Hour of Day Consistency
        CV = (std_dev_hours / mean_hours)
        time_consistency = round(1 - CV, 3)
        time_consistency = pd.DataFrame(time_consistency, columns=['Time Consistency']).reset_index()
        time_consistency = time_consistency.replace([-np.inf], 0)
        return time_consistency
    
    def find_work_AOI(self, breakdown_of_time_per_location_per_dow, potential_work_locations):
        breakdown_of_time_per_location_per_dow = breakdown_of_time_per_location_per_dow.reset_index()
        avg_hours_per_dow = breakdown_of_time_per_location_per_dow.drop(columns=['Date']).groupby('Day of week').mean()
        avg_hours_per_dow = avg_hours_per_dow.reset_index(drop=True)
        avg_hours_per_dow = avg_hours_per_dow.T
        avg_hours_per_dow = avg_hours_per_dow.reset_index()

        # Vectorized calculation for points based on values between 3 and 9
        dow_count_where_hours_between_3_9 = np.where(np.maximum(0, np.minimum(avg_hours_per_dow.iloc[:, 1:6] - 3, 6)) != 0, 10, 0)
        weekday_points = dow_count_where_hours_between_3_9.sum(axis=1)

        # Check if average hours for location are 0 on day of week 5 and 6
        try:
            points_dow_5 = np.where(avg_hours_per_dow[5] == 0, 25, 0)
        except:
            points_dow_5 = 0

        try:
            points_dow_6 = np.where(avg_hours_per_dow[6] == 0, 25, 0)
        except:
            points_dow_6 = 0

        # If average 'working' hours during the weekdays are more than 3, assign the weekend points (points for having 0 working hours on the weekend)
        # Else, the weekend points are 0
        weekend_points = np.where(np.mean(avg_hours_per_dow.iloc[:, 1:6], axis=1) >= 3, points_dow_5 + points_dow_6, 0)

        # Compute the Work Likelihood for each location
        avg_hours_per_dow['Work Likelihood'] = weekday_points + weekend_points

        # Initialize the workplace variable
        workplace = None

        # Find the max value in Work Likelihood
        max_workplace_likelihood = avg_hours_per_dow['Work Likelihood'].max()

        # If it's 100 and there's only 1 value
        if max_workplace_likelihood == 100 and avg_hours_per_dow['Work Likelihood'].eq(100).sum() == 1:
            location = avg_hours_per_dow.loc[avg_hours_per_dow['Work Likelihood'].idxmax(), 'Location']
            workplace = {'LOCATION': location, 'TYPE': 'Workplace'}

        # If it's above 50 and there's only 1 value
        elif max_workplace_likelihood >= 50 and avg_hours_per_dow['Work Likelihood'].eq(max_workplace_likelihood).sum() == 1:
            location = avg_hours_per_dow.loc[avg_hours_per_dow['Work Likelihood'].idxmax(), 'Location']
            workplace = {'LOCATION': location, 'TYPE': 'Potential Workplace'}

        # If there are multiple locations with the highest work likelihood
        elif max_workplace_likelihood >= 50:
            potential_work_locations = avg_hours_per_dow.loc[avg_hours_per_dow['Work Likelihood'] == max_workplace_likelihood, 'Location'].tolist()
            time_consistency = self.study_time_consistency(potential_work_locations)
            avg_hours_per_dow = pd.merge(avg_hours_per_dow, time_consistency, on='Location', how='left')
            avg_hours_per_dow['Work Likelihood'] *= avg_hours_per_dow['Time Consistency']
            display(avg_hours_per_dow)
            workplace = {'LOCATION': avg_hours_per_dow.loc[avg_hours_per_dow['Work Likelihood'].idxmax(),'Location'], 'TYPE': 'Potential Workplace'}

        # If there's not any location with a work likelihood >= 50
        else:
            workplace = {'LOCATION': None, 'TYPE': None}
            
        self.result_AOI['Centroids'] = [x for x in zip(self.result_AOI['LAT'], self.result_AOI['LNG'])]
        self.result_AOI.loc[self.result_AOI['Centroids'] == workplace['LOCATION'], 'TYPE'] = workplace['TYPE']
        self.result_AOI = self.result_AOI.drop(columns=['Centroids'])

    def identify_work_AOIs(self):
        potential_work_locations = self.filter_potential_work_locations()
        breakdown_of_time_per_location_per_dow = self.breakdown_of_time_per_location_per_dow(potential_work_locations)
        self.find_work_AOI(breakdown_of_time_per_location_per_dow, potential_work_locations)
        
    def evaluate_confidence_in_analysis(self):
        tmp_hits_per_day = self.data.copy()
        tmp_hits_per_day['Date'] = tmp_hits_per_day['Timestamp'].dt.date
        tmp_hits_per_day['Hour of Day'] = tmp_hits_per_day['Timestamp'].dt.hour
        tmp_hits_per_day['Day of Week'] = tmp_hits_per_day['Timestamp'].dt.dayofweek

        try:
            N_recorded_days = self.unique_dates['Date'].nunique()
            N_expected_days = (self.max_date - self.min_date).days + 1
            
            # Compute Time Period Consistency
            TP_consistency = N_recorded_days / N_expected_days
        except:
            TP_consistency = 0
        
        try:
            date_range = pd.date_range(self.min_date, self.max_date, freq='D').date
            dates_df = pd.DataFrame({'Date': date_range})

            # Compute hits per day, even for dates that have no hits within time period
            hits_per_day = (dates_df.merge(tmp_hits_per_day, how='left', left_on='Date', right_on='Date').groupby('Date').size().fillna(0).astype(int))

            # Compute mean and standard deviation of hits
            avg_hits_per_day, std_dev_hits_per_day = hits_per_day.apply(['mean', 'std'])

            # Compute Coefficient of Variation: CV = SD / M
            CV = std_dev_hits_per_day / avg_hits_per_day

            # Compute Hits Per Day Consistency
            HD_consistency = round(1 - CV, 3)
        except:
            HD_consistency = 0
        
        try: 
            hours_df = pd.DataFrame({'Hour of Day': list(range(24))})

            ### Compute hits per hour, even for hour that doesn't have hits
            hits_per_hour = (hours_df.merge(tmp_hits_per_day, how='left', on='Hour of Day').groupby('Hour of Day').size().fillna(0).astype(int))
            mean_hits_per_hour, std_dev_hits_per_hour = hits_per_hour.apply(['mean', 'std']).apply(round)

            ### Compute Entropy of Hour of Day Distribution
            CV = (std_dev_hits_per_hour / mean_hits_per_hour)
            HOD_consistency = round(1 - CV, 3)
        except:
            HOD_consistency = 0
       
        try:
            dow_df = pd.DataFrame({'Day of Week': list(range(7))})
            
            # Compute hits per hour, even for hour that doesn't have hits
            hits_per_dow = (dow_df.merge(tmp_hits_per_day, how='left', on='Day of Week').groupby('Day of Week').size().fillna(0).astype(int))
            avg_hits_per_dow, std_dev_hits_per_dow = hits_per_dow.apply(['mean', 'std']).apply(round)

            # Compute Coefficient of Variation of DOW Distribution
            CV = (std_dev_hits_per_dow / avg_hits_per_dow)

            # Computing Hour of Day Consistency
            DOW_consistency = round(1 - CV, 3)
        except:
            DOW_consistency = 0
        
        # Specifying parameter weigths
        wTP, wHD, wHOD, wDOW = 0.25, 0.25, 0.25, 0.25
        N_hits = self.data.shape[0]
        N_days = N_recorded_days
        acceptable_N_hits = 20000 
        acceptable_N_days = 60 

        if N_days == 1:
            CS, adjusted_CS = 0, 0
        else:
            # Computing the adjustment factors for number of days and NUMBER_OF_RECORDED_HITS
            N_hits_adj_factor = round(min(math.log(N_hits) / math.log(acceptable_N_hits), 1), 2)
            N_days_adj_factor = round(min(math.log(N_days) / math.log(acceptable_N_days), 1), 2)

            # Computing the overall Consistency Score
            CS = ((wTP * TP_consistency) + (wHD * HD_consistency) + (wHOD * HOD_consistency) + (wDOW * DOW_consistency)) / (wTP + wHD + wHOD + wDOW)
            CS = round(CS, 3)

            # Normalizing the adjusted CS Score between 0 and 1
            adjusted_CS = round(CS * N_hits_adj_factor * N_days_adj_factor, 3)

        # Convert CS and Adjusted CS to 0 if they're NaN
        CS = CS if not pd.isna(CS) else 0
        adjusted_CS = adjusted_CS if not pd.isna(adjusted_CS) else 0

        CS = max(CS, 0)
        adjusted_CS = max(adjusted_CS, 0)

        new_row = {'Device_ID_geo': self.device_id_geo, 
                    'Min_Date': self.min_date.date(), 
                    'Max_Date': self.max_date.date(),
                    'NUMBER_OF_RECORDED_DAYS': N_days, 
                    'NUMBER_OF_RECORDED_HITS': N_hits,
                    'Consistency_Score': CS,
                    'Adjusted_Consistency_Score': adjusted_CS}
        
        self.aoi_confidence_results = pd.concat([self.aoi_confidence_results, pd.DataFrame([new_row])], ignore_index=True)