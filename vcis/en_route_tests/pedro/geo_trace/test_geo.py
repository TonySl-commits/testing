import pandas as pd
import folium 

from cdr_trace.reporting.geo_reporting.aoi_report.aoi_report_functions import AOIReportFunctions

def get_geo_history(geolocation_analyzer):
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        all_trajectories_with_device_id = []

        for device in range(geolocation_analyzer.geolocation_data_list.get_length()):
            geolocation_data = geolocation_analyzer.geolocation_data_list[device]

            aoi_report = AOIReportFunctions()
            aoi_report.initialize_aoi_report(geolocation_data)
            location_df = aoi_report.get_locations_df()

            
            
            location_df['Location'] = location_df['Location'].astype(str)
            location_df['Timestamp'] = pd.to_datetime(location_df['Timestamp'])
            location_df.sort_values(by='Timestamp', inplace=True)

            # save location_df in the desktop
            location_df.to_csv(f'/u01/jupyter-scripts/Pedro/CDR_Trace_N/CDR_Trace/data/dataframes/location_df_{aoi_report.geolocation_data.device_id_geo}.csv')


            # print(f"LOCATION DF OF {aoi_report.geolocation_data.device_id_geo} DEVICE:")
            # print(location_df)
            # print("COLUMNS:")
            # print(location_df.columns)
            # print("SHAPE:")
            # print(location_df.shape)
            # print("Special Columns:")
            # print(location_df[["Latitude","Longitude","Location","Timestamp"]])
