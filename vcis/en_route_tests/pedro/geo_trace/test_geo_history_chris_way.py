import pandas as pd
import folium 

from cdr_trace.reporting.geo_reporting.aoi_report.aoi_report_functions import AOIReportFunctions


def get_geo_history(geolocation_analyzer):
    all_trajectories_with_device_id = []

    for device in range(geolocation_analyzer.geolocation_data_list.get_length()):
        geolocation_data = geolocation_analyzer.geolocation_data_list[device]

        aoi_report = AOIReportFunctions()
        aoi_report.initialize_aoi_report(geolocation_data)
        location_df = aoi_report.get_locations_df()

        location_df['Location'] = location_df['Location'].astype(str)
        location_df['Timestamp'] = pd.to_datetime(location_df['Timestamp'])
        location_df.sort_values(by='Timestamp', inplace=True)

        # Create a shifted version of the 'Location' column to compare with the next row
        location_df['Next_Location'] = location_df['Location'].shift(-1)
        location_df['Prev_Location'] = location_df['Location'].shift()

        # Mark rows where the location changes to a different AOI (not back to the same one)
        location_df['Is_Transition'] = ((location_df['Location'] != location_df['Next_Location']) &
                                        (location_df['Location'] != "Other") & 
                                        (location_df['Next_Location'] != "Other")) | \
                                        ((location_df['Location'] != location_df['Prev_Location']) &
                                        (location_df['Location'] != "Other") & 
                                        (location_df['Prev_Location'] != "Other") & 
                                        (location_df['Prev_Location'].notna()))

        # Find the indexes where transitions occur
        transition_indexes = location_df[location_df['Is_Transition']].index.tolist()

        # Print the indexes
        print(f"Transition indexes of {aoi_report.geolocation_data.device_id_geo}:")
        print(transition_indexes)

        # Use indexes to segment the DataFrame and store each trajectory
        start_idx = 0
        for end_idx in transition_indexes + [len(location_df) - 1]:  # Ensure the last segment is included
            # Extract the trajectory segment
            trajectory_df = location_df.iloc[start_idx:end_idx + 1]
            if not trajectory_df.empty:
                all_trajectories_with_device_id.append((trajectory_df, geolocation_data.device_id_geo))
            start_idx = end_idx + 1

        print(f"Number of trajectories with device ID {geolocation_data.device_id_geo}: {len(all_trajectories_with_device_id)}")
    
    print("ALL TRAJECTORIES WITH DEVICE ID:")
    print(all_trajectories_with_device_id)
    
    # After collecting all trajectories with their device IDs, create maps
    # create_map_history_geo(all_trajectories_with_device_id)   

def create_map_history_geo(all_trajectories_with_device_id):
    # Iterate over each trajectory and its corresponding device ID
    for index, (trajectory_df, device_id) in enumerate(all_trajectories_with_device_id):
        # Assuming the DataFrame is not empty and has the required columns
        if not trajectory_df.empty:
            # Initialize the map centered on the first point of the trajectory
            initial_location = [trajectory_df.iloc[0]['Latitude'], trajectory_df.iloc[0]['Longitude']]
            m = folium.Map(location=initial_location, zoom_start=12)
            
            # Plot the first point with a special marker (e.g., green)
            first_point = trajectory_df.iloc[0]

            folium.CircleMarker(
                location=[first_point['Latitude'], first_point['Longitude']],
                radius=50,
                color='darkblue',
                fill=True,
                fill_color='blue',
                fill_opacity=0.3,
                tooltip=f"COORDINATES: ({first_point['Latitude']}, {first_point['Longitude']})<br>Timestamp: {first_point['Timestamp']}"
            ).add_to(m)
            
            # Plot intermediate points with blue markers
            for _, point in trajectory_df.iloc[1:-1].iterrows():
                folium.Marker(
                    location=[point['Latitude'], point['Longitude']],
                    popup=f"Time: {point['Timestamp']}",
                    icon=folium.Icon(color='blue', icon='info-sign')
                ).add_to(m)
            
            # Plot the last point with a special marker (e.g., red)
            last_point = trajectory_df.iloc[-1]
            folium.Marker(
                location=[last_point['Latitude'], last_point['Longitude']],
                popup=f"End: {last_point['Timestamp']}",
                icon=folium.Icon(color='red', icon='stop')
            ).add_to(m)
            
            # Save the map to an HTML file
            map_filename = f"trajectory_{index+1}_device_{device_id}.html"
            map_path = f"/u01/jupyter-scripts/Pedro/CDR_Trace_N/CDR_Trace/data/maps/history_geo/{map_filename}"
            m.save(map_path)
            print(f"Map saved to: {map_path}")

    

