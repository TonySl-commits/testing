import pandas as pd
import numpy as np

from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.engine_break_exception import EngineBreakException
from vcis.utils.data_processing_utils import DataProcessing


class GeospatialConfidenceAnalysis:
    def __init__(self):
        # Engine Parameters
        self.ACCEPTABLE_NUMBER_OF_DAYS = 60
        self.GEO_START_HOUR = 6
        self.BINS_PER_HOUR = 6

        # Configure metric weights
        self.metric_weights = {
            'default': {
                'wTP': 0.25,
                'wHD': 0.25,
                'wHOD': 0.25,
                'wDOW': 0.25
            },                            
            'Activity Scan By Hits': {
                'wTP': 0,
                'wHD': 0,
                'wHOD': 1,
                'wDOW': 0
            }
        }

        # Configure analysis settings
        self.analysis_settings = {
            'default': {
                'TIME_PERIOD_CONSISTENCY' : True,
                'HOUR_OF_DAY_CONSISTENCY' : True,
                'DAY_OF_WEEK_CONSISTENCY' : True,
                'HITS_PER_DAY_CONSISTENCY' : True,
                'DAYS_ADJUSTMENT_FACTOR' : True
            },
            'Activity Scan By Hits': {
                'TIME_PERIOD_CONSISTENCY' : False,
                'HOUR_OF_DAY_CONSISTENCY' : True,
                'DAY_OF_WEEK_CONSISTENCY' : False,
                'HITS_PER_DAY_CONSISTENCY' : False,
                'DAYS_ADJUSTMENT_FACTOR' : False
            }
        }

        # Metric weight mapping
        self.metric_weight_mapping = {
            'TIME_PERIOD_CONSISTENCY': 'wTP',
            'HOUR_OF_DAY_CONSISTENCY': 'wHOD',
            'DAY_OF_WEEK_CONSISTENCY': 'wDOW',
            'HITS_PER_DAY_CONSISTENCY': 'wHD'
        }

        # Simulation Time Period
        self.simulation_start_date = None
        self.simulation_end_date = None
        self.simulation_time_period = None
        
        # Tools
        self.properties = CDR_Properties()
        self.utils = CDR_Utils()
        self.oracle_tools = OracleTools()
        self.data_processor = DataProcessing()

    def configure_analysis_settings(self, time_period, analysis_mode='default', verbose=False):
        
        
        print(f'INFO:       Configuring analysis settings...')
        print(time_period['START_DATE'], time_period['END_DATE'])

        if type(time_period['START_DATE']) == str:
            time_period['START_DATE'] = pd.to_datetime(time_period['START_DATE'])
        if type(time_period['END_DATE']) == str:
            time_period['END_DATE'] = pd.to_datetime(time_period['END_DATE'])
        
        self.simulation_start_date = time_period['START_DATE']
        self.simulation_end_date = time_period['END_DATE']       
        print(type(self.simulation_start_date), type(self.simulation_end_date))
        # Get the number of days between the simulation start and end dates
        simulation_time_period = (self.simulation_end_date - self.simulation_start_date).days + 1
        
        if verbose: 
            print(f'INFO:       Simulation time period: {simulation_time_period}')

        if simulation_time_period > 1:
            self.analysis_settings[analysis_mode]['TIME_PERIOD_CONSISTENCY'] = True
            self.analysis_settings[analysis_mode]['HITS_PER_DAY_CONSISTENCY'] = True
            self.analysis_settings[analysis_mode]['DAY_OF_WEEK_CONSISTENCY'] = True\

            self.metric_weights[analysis_mode]['wHD'] = 0.25
            self.metric_weights[analysis_mode]['wDOW'] = 0.25
            self.metric_weights[analysis_mode]['wHOD'] = 0.25
            self.metric_weights[analysis_mode]['wTP'] = 0.25


        return

    def preprocess_data(self, data, analysis_mode='default', verbose=False):

        if analysis_mode == 'Activity Scan By Hits':
            # Remove unnecessary columns
            data = self.data_processor.remove_unnecessary_columns(
                data, 
                remove_zeros=True, 
                remove_columns=['USAGE_DATE', 'CALLED_NO', 'CALLING_NO', 'LOCATION_ACCURACY', 'LOCATION_NAME']
            )

            # Rename columns
            data.rename(columns={
                'DEVICE_ID':'DEVICE_ID_GEO',
                'USAGE_TIMEFRAME':'Timestamp', 
                'LOCATION_LATITUDE':'Latitude',
                'LOCATION_LONGITUDE':'Longitude'}, inplace=True)
            
        if analysis_mode == 'default':
            # Rename columns
            data.rename(columns={
                'Device_ID_geo':'DEVICE_ID_GEO',
                'usage_timeframe':'Timestamp'}, inplace=True)
        
        # Convert the timestamp column to datetime if needed
        data['Timestamp'] = pd.to_datetime(data['Timestamp'], unit='ms')
        
        # Extract date, hour of day, and day of week
        data['Date'] = data['Timestamp'].dt.date
        data['HourOfDay'] = data['Timestamp'].dt.hour
        data['DayOfWeek'] = data['Timestamp'].dt.dayofweek  # Monday=0, Sunday=6  

        # Bin the data into 6 intervals of 10 minutes each
        data['TimeBin'] = pd.cut(data['Timestamp'].dt.minute, bins=self.BINS_PER_HOUR, labels=False)
            
        if verbose:
            print("\nINFO:    Preprocessed Data: \n")
            self.data_processor.print_table(data)    
            
        return data
    
    def prepare_preliminary_dictionary(self, data):
        # Get the device id
        device_id_geo = str(data['DEVICE_ID_GEO'].unique().tolist()[0]) if data['DEVICE_ID_GEO'].nunique() == 1 else 'None'

        # Extract the minimum and maximum timestamps from the data
        start_date = data['Date'].min()
        end_date = data['Date'].max()
        
        # Compute the number of recorded days
        recorded_days = data['Date'].nunique()

        # Get the number of recorded hits
        recorded_hits = data.shape[0]

        # Prepare preliminary dictionary
        confidence_analysis = {
            'DEVICE_ID_GEO': device_id_geo,
            'START_DATE': start_date, 
            'END_DATE': end_date,
            'RECORDED_DAYS': recorded_days, 
            'RECORDED_HITS': recorded_hits
        }
        
        return confidence_analysis
    
    def time_period_consistency(self, recorded_days):      
        # Compute the number of days between the first and last timestamp
        expected_days = (self.simulation_end_date - self.simulation_start_date).days + 1
        
        # Compute the time period consistency as a percentage
        time_period_consistency = (recorded_days / expected_days) * 100
        
        # Round to 2 decimal places
        time_period_consistency = round(time_period_consistency, 2)

        return time_period_consistency
    
    def compute_day_of_week_quality_factor(self, data):        
        # Count the number of bins with hits for each date, day of the week, and hour of the day
        hits_per_hour_qf = data.groupby(['Date', 'DayOfWeek', 'HourOfDay'])['TimeBin'].nunique()
        
        # Compute the average count of hits per day of the week, hour of the day
        average_hits_per_hour_qf = hits_per_hour_qf.groupby(['Date', 'DayOfWeek', 'HourOfDay']).sum() / self.BINS_PER_HOUR

        # Rename the resulting column to 'QualityFactor'
        average_hits_per_hour_qf = average_hits_per_hour_qf.rename('QualityFactor')

        # Reset index and filter hour of day after 6 am
        average_hits_per_hour_qf = average_hits_per_hour_qf.reset_index()
        average_hits_per_hour_qf = average_hits_per_hour_qf[average_hits_per_hour_qf['HourOfDay'] >= self.GEO_START_HOUR]
        
        # Average Quality Factor per Date
        average_hits_per_hour_qf = average_hits_per_hour_qf.groupby(['Date', 'DayOfWeek'])['QualityFactor'].sum() / (24 - self.GEO_START_HOUR)

        # Average with respect to Day of Week
        average_hits_per_hour_qf = average_hits_per_hour_qf.groupby(['DayOfWeek']).mean()
        
        return average_hits_per_hour_qf

    def day_of_week_consistency(self, data):
        # Compute the quality factor
        average_hits_per_hour_qf = self.compute_day_of_week_quality_factor(data)
        
        # Compute the number of days for each day of the week
        day_of_week_count = data.groupby('DayOfWeek')['Date'].nunique()
        
        # Multiply the number of days by the respective quality factor
        day_of_week_consistency_results = day_of_week_count * average_hits_per_hour_qf
        
        # Reindex day of week and fill empty values with 0
        day_of_week_consistency_results = day_of_week_consistency_results.reindex(range(7), fill_value=0)
        
        # Compute the standard deviation with respect to the day of the week
        std_dev_day_of_week = day_of_week_consistency_results.std()
        
        # Compute the day of week consistency
        day_of_week_consistency = 100 / (1 + std_dev_day_of_week)

        # Round to 2 decimal places
        day_of_week_consistency = round(day_of_week_consistency, 2)
        
        return day_of_week_consistency

    def compute_hour_of_day_quality_factor(self, data):        
        # Count the number of bins with hits for each date and hour of day
        hits_per_hour_qf = data.groupby(['Date','HourOfDay'])['TimeBin'].nunique()
        
        # Compute the average count of hits per day of the week, hour of day
        average_hits_per_hour_qf = hits_per_hour_qf.groupby(['Date','HourOfDay']).sum() / self.BINS_PER_HOUR

        # Reset index and filter hour of day after 6 am
        average_hits_per_hour_qf = average_hits_per_hour_qf.reset_index()
        average_hits_per_hour_qf = average_hits_per_hour_qf[average_hits_per_hour_qf['HourOfDay'] >= self.GEO_START_HOUR]

        # Average with respect to Hour of Day
        average_hits_per_hour_qf = average_hits_per_hour_qf.groupby(['HourOfDay'])['TimeBin'].mean()
        average_hits_per_hour_qf = average_hits_per_hour_qf.reindex(range(self.GEO_START_HOUR, 24), fill_value=0)
        
        return average_hits_per_hour_qf

    def hour_of_day_consistency(self, data):
        # Compute the quality factor
        average_hits_per_hour_qf = self.compute_hour_of_day_quality_factor(data)
        
        # Compute the number of days for each day of the week
        hour_of_day_count = data[data['HourOfDay'] >= self.GEO_START_HOUR].groupby('HourOfDay')['Date'].nunique()
        hour_of_day_count = hour_of_day_count.reindex(range(self.GEO_START_HOUR, 24), fill_value=0)

        # Multiply the number of days by the respective quality factor
        hour_of_day_consistency_results = average_hits_per_hour_qf * hour_of_day_count

        # Compute the standard deviation with respect to the day of the week
        std_dev_hour_of_day = hour_of_day_consistency_results.std()
        
        # Compute the day of week consistency
        hour_of_day_consistency = 100 / (1 + std_dev_hour_of_day)

        # Round to 2 decimal places
        hour_of_day_consistency = round(hour_of_day_consistency, 2)
        
        return hour_of_day_consistency
    
    def compute_hits_per_day_quality_factor(self, data, dates_within_period):        
        # Count the number of bins with hits for each date and hour of day
        hits_per_day_qf = data.groupby(['Date','HourOfDay'])['TimeBin'].nunique()
        
        # Compute the average count of hits per day, hour of day
        average_hits_per_day_qf = hits_per_day_qf.groupby(['Date','HourOfDay']).sum() / self.BINS_PER_HOUR

        # Reset index and filter hour of day after the start hour
        average_hits_per_day_qf = average_hits_per_day_qf.reset_index()
        average_hits_per_day_qf = average_hits_per_day_qf[average_hits_per_day_qf['HourOfDay'] >= self.GEO_START_HOUR]

        # Average across period of time
        average_hits_per_day_qf = average_hits_per_day_qf.groupby(['Date'])['TimeBin'].mean()
        average_hits_per_day_qf = average_hits_per_day_qf.reindex(dates_within_period, fill_value=0)
        
        return average_hits_per_day_qf

    def hits_per_day_consistency(self, data, start_date, end_date):
        # Generate dates within the period
        dates_within_period = pd.date_range(start=start_date, end=end_date)

        # Compute the quality factor
        average_hits_per_day_qf = self.compute_hits_per_day_quality_factor(data, dates_within_period)
        
        # Compute the number of days for each day of the week
        hits_per_day = data.groupby('Date')['DayOfWeek'].nunique()

        hits_per_day = hits_per_day.reindex(dates_within_period, fill_value=0)

        # Multiply the number of days by the respective quality factor
        hits_per_day_consistency_results = average_hits_per_day_qf * hits_per_day

        # Compute the standard deviation with respect to the day of the week
        std_dev_hour_of_day = hits_per_day_consistency_results.std()
        
        # Compute the day of week consistency
        hits_per_day_consistency = 100 / (1 + std_dev_hour_of_day)

        # Round to 2 decimal places
        hits_per_day_consistency = round(hits_per_day_consistency, 2)
        
        return hits_per_day_consistency

    def days_adjustment_factor(self, recorded_days):
        # Compute days adjustment factor
        days_adjustment_factor = np.log1p(recorded_days) / np.log(self.ACCEPTABLE_NUMBER_OF_DAYS)

        return days_adjustment_factor

    def compute_consistency_metrics(self, data, confidence_analysis, analysis_mode='default', verbose=False):
        # Initialize consistency metrics dictionary
        consistency_metrics = {}

        if self.analysis_settings[analysis_mode]['TIME_PERIOD_CONSISTENCY']:
            # Measure time period consistency
            time_period_consistency = self.time_period_consistency(confidence_analysis['RECORDED_DAYS'])

            consistency_metrics['TIME_PERIOD_CONSISTENCY'] = time_period_consistency
        
        if self.analysis_settings[analysis_mode]['HOUR_OF_DAY_CONSISTENCY']:
            # Measure hour of day consistency
            hour_of_day_consistency = self.hour_of_day_consistency(data)

            consistency_metrics['HOUR_OF_DAY_CONSISTENCY'] = hour_of_day_consistency
        
        if self.analysis_settings[analysis_mode]['DAY_OF_WEEK_CONSISTENCY']:
            # Measure day of week consistency
            day_of_week_consistency = self.day_of_week_consistency(data)

            consistency_metrics['DAY_OF_WEEK_CONSISTENCY'] = day_of_week_consistency
        
        if self.analysis_settings[analysis_mode]['HITS_PER_DAY_CONSISTENCY']:
            # Measure hits per day consistency
            hits_per_day_consistency = self.hits_per_day_consistency(data, 
                                                                    confidence_analysis['START_DATE'], 
                                                                    confidence_analysis['END_DATE'])

            consistency_metrics['HITS_PER_DAY_CONSISTENCY'] = hits_per_day_consistency

        return consistency_metrics

    
    def _confidence_score(self, consistency_metrics, analysis_mode='default'):
        # Specify parameter weigths
        metric_weights = self.metric_weights[analysis_mode]

        # Sum the available consistency metrics multiplied by their weight using the metric weight mapping
        confidence_score = sum(metric_weights[self.metric_weight_mapping[metric]] * consistency_metrics[metric] for metric in consistency_metrics.keys())

        return confidence_score
    
    def compute_confidence_score(self, consistency_metrics, confidence_analysis, analysis_mode='default'):
        # Get confidence score
        confidence_score = self._confidence_score(consistency_metrics, analysis_mode=analysis_mode)

        # Apply adjustment factor if needed
        if self.analysis_settings[analysis_mode]['DAYS_ADJUSTMENT_FACTOR']:
            # Compute the days adjustment factor
            days_adjustment_factor = self.days_adjustment_factor(confidence_analysis['RECORDED_DAYS'])

            # Adjust confidence score
            confidence_score = confidence_score * days_adjustment_factor 

        # Round to 2 decimal places
        confidence_score = round(confidence_score, 2)

        return confidence_score
    
    def evaluate_confidence_in_analysis(self, data, analysis_mode='default', test_mode=True, verbose=False):
        # Prepare preliminary data dictionary
        confidence_analysis = self.prepare_preliminary_dictionary(data)

        # Compute consistency metrics
        consistency_metrics = self.compute_consistency_metrics(
            data, 
            confidence_analysis,
            analysis_mode=analysis_mode
        )

        # Compute the adjusted confidence score
        confidence_score = self.compute_confidence_score(
            consistency_metrics, 
            confidence_analysis,
            analysis_mode=analysis_mode
        )

        confidence_analysis = {**confidence_analysis, **consistency_metrics}
        confidence_analysis['CONFIDENCE_SCORE'] = confidence_score

        if verbose and test_mode:
            print('\nINFO:    CONFIDENCE ANALYSIS RESULTS:')
            max_key_length = max(len(key) for key in confidence_analysis.keys())
            for key, value in confidence_analysis.items():
                print(f'      {key}'.ljust(max_key_length + 10), value)
        
        confidence_analysis = pd.DataFrame([confidence_analysis])

        return confidence_analysis
    
    def evaluate_confidence_in_analysis_for_all_devices(self, data, batch_mode = False, analysis_mode='default', verbose=False):
        # Initialize result dataframe list
        result_df_list = []

        if batch_mode:

            if verbose:
                print('INFO:          Batch Mode Activated!')

            # Evaluate confidence in analysis for all devices in one go
            result_df = self.evaluate_confidence_in_analysis(data, 
                                                             analysis_mode=analysis_mode, 
                                                             verbose=verbose)
            result_df_list.append(result_df)

        else:

            # Group by device_id and apply evaluate_confidence_in_analysis
            for device_id, group in data.groupby(['DEVICE_ID_GEO']):

                if verbose:
                    print('INFO:          Evaluating Confidence for Device: ', device_id)

                # Evaluate the confidence in analysis for each device
                result_df = self.evaluate_confidence_in_analysis(group, 
                                                                analysis_mode=analysis_mode, 
                                                                verbose=verbose)
                
                # Append the result to the result dataframe list
                result_df_list.append(result_df)

        # Concatenate the results
        final_result = pd.concat(result_df_list, ignore_index=True)
            
        return final_result

    def evaluate_confidence_in_analysis_wrt_service_provider(self, simulation_table, time_period=None, batch_mode=False, analysis_mode='default', verbose=False):
        # Initialize result dataframe
        confidence_analysis_per_service_provider = {}

        # Group the simulation table by service provider
        for service_provider, group in simulation_table.groupby(['SERVICE_PROVIDER_ID']):

            if verbose:
                print('\nINFO:    Evaluating Confidence for Service Provider: ', service_provider, '\n')
                  
            # Evaluate the confidence in analysis for each device
            confidence_analysis = self.evaluate_confidence_in_analysis_for_all_devices(
                group, 
                batch_mode=batch_mode,
                analysis_mode=analysis_mode, 
                verbose=verbose
            )
            
            confidence_analysis_per_service_provider[service_provider] = confidence_analysis

        # Add the service_provider column to each dataframe and store them in a list
        result_df_list = []
        for service_provider_id, df in confidence_analysis_per_service_provider.items():
            df['SERVICE_PROVIDER_ID'] = service_provider_id
            result_df_list.append(df)

        # Concatenate all the dataframes in the result_df_list into one
        final_result = pd.concat(result_df_list, ignore_index=True)

        # Put the column SERVICE_PROVIDER_ID as the first column
        cols = list(final_result.columns)
        cols = cols[-1:] + cols[:-1]
        final_result = final_result[cols]
        
        return final_result
    
    def perform_confidence_test(self, data, test_id, time_period = None, analysis_mode='default', verbose=False):
        
        if time_period is None:
            raise EngineBreakException('ERROR:  Time Period Not Specified')
        
        # Configure the analysis settings
        self.configure_analysis_settings(time_period, analysis_mode=analysis_mode, verbose=verbose) 
    
        # Preprocess the data
        data = self.preprocess_data(data, analysis_mode=analysis_mode, verbose=verbose)

        # Perform the confidence test
        if analysis_mode == 'default':
            confidence_analysis_result = self.evaluate_confidence_in_analysis_for_all_devices(data, 
                                                                                              analysis_mode=analysis_mode,
                                                                                              verbose=verbose)
        
        elif analysis_mode == 'Activity Scan By Hits':
            confidence_analysis_result = self.evaluate_confidence_in_analysis_wrt_service_provider(data,                                                                                               analysis_mode=analysis_mode,
                                                                                                   batch_mode=True,
                                                                                                   verbose=verbose)
        else:
            raise EngineBreakException('ERROR:  Invalid Analysis Mode')
        
                
        # Save the results in the database
        self.oracle_tools.drop_create_insert(confidence_analysis_result, 
                                            self.properties.geo_confidence_analysis_table, 
                                            test_id, 
                                            self.properties._oracle_table_schema_query_geo_confidence_analysis)

        return confidence_analysis_result