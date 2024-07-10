from vcis.triangulation.triangulation_functions import TriangulationFunctions
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
import pandas as pd
import folium
import numpy as np
triangulation = TriangulationFunctions()
cassandra_spark_tools = CassandraSparkTools()
utils = CDR_Utils()
properties = CDR_Properties()
#before merge

##########################################################################################################################################################
    
imsi_id = "121415223435890"
start_date = "2023-03-11"
end_date ="2024-02-04"
server= "10.1.10.66"
start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(start_date)

# #Pre-work
df = cassandra_spark_tools.get_device_history_imsi_spark(imsi_id,start_date,end_date,server)
csv_file_path = "df.csv"
df.to_csv(csv_file_path, index=False)
#print(f"This is df, the dataset that is within the time slots saved to {csv_file_path}")

bts_spark= cassandra_spark_tools.get_spark_data(properties.bts_table_name,passed_connection_host = server)
bts = cassandra_spark_tools.get_device_history_imsi_spark(imsi_id,start_date,end_date,server)
bts = bts_spark.toPandas()
csv_file_path = "bts.csv"
bts.to_csv(csv_file_path, index=False)
#print(f"This is df, the dataset that is within the time slots saved to {csv_file_path}")


df =triangulation. merge_datasets(df, bts)
df = triangulation.transform_dataframe(df)
print(f"The head of df is ")
print(df.head())



print("YESSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
##########################################################################################################################################################

#Main
#Pre-work
#file_path = input("Enter the file path: ")

#df = pd.read_csv(file_path)  

#Function 1 
unique_dates_with_days = triangulation.extract_and_return_unique_dates_with_day(df)
#Below function returns the unique date (day-month-year) of the dataset entered
#It recommends the dates (day) that the user choose to enter
print(unique_dates_with_days)
print("\n\n")

#Function 2
#This function asks the user to enter the threshold (distance and time) values in which he will measure the triangulation
processed_data, input_date = triangulation.preprocess_and_filter_data(df)
processed_data_filtered, distance_threshold, time_threshold = triangulation.get_filtered_data(processed_data, input_date)
print("\n\n")




#Function 3
data = triangulation.compute_triangulation_data(processed_data_filtered, distance_threshold, time_threshold)   
#Below shows the shape and head of the "data" dataset containing records that are close and almost subsequent 
#Based on time, distance, and naming,values without considering azimuth), possibly forming a triangulation but without considering azimuth
print("The head of data dataset: \n",data.head())
print("The shape of data dataset: \n", data.shape)
print("\n\n")

#Function 4 - Parallel
true_df, true_count, false_count = triangulation.get_true_df_and_counts(data)
par_filtered_df, unique_locations_count = triangulation.filter_and_get_unique_locations(true_df)
#Print additional details
print("The shape of par_filtered_df: \n", par_filtered_df.shape)
print("The head of par_filtered_df is: \n", par_filtered_df.head())
print("The number of unique location values in the parallel approach of possible triangulation is: \n", unique_locations_count)
print("Below are the intersection coordinates values of the points of every pair forming the intersection in the parallel approach: ")    
m_parallel,intersection_coordinates_list= triangulation.create_map_and_plot_data_parallel(par_filtered_df)
print("\nThe list of unique points overall of the intersection_coordinates_list is: ")
triangulation.print_intersection_coordinates(intersection_coordinates_list)  
if triangulation.display_map:
    m_parallel.save(properties.passed_filepath_tmaps+"m_parallel.html")
print("\n")
print("The parallel approach is done and the map has been saved to m_parallel.html")
print("\n\n")


#Function 5- Oppose
#data_oppose = compute_triangulation_data(processed_data_filtered, distance_threshold, time_threshold)
true_df_opp = triangulation.calculate_counts_and_true_df_oppose(data)  # Update to use the opposing function
opp_filtered_df, unique_locations_count_oppose = triangulation.filter_and_get_unique_locations_oppose(true_df_opp)
print("The shape of opp_filtered_df: \n", opp_filtered_df.shape)
print("The head of par_filtered_df is: \n", opp_filtered_df.head())
print("The number of unique location values in the oppose approach of possible triangulation is: \n", unique_locations_count_oppose)
print("Below are the intersection coordinates values of the points of every pair forming the intersection in the parallel approach: ")    
#m_oppose,intersection_coordinates_list= create_map_and_plot_data_parallel(par_filtered_df)
m_oppose, intersection_coordinates_list_opp = triangulation.create_map_and_plot_data_oppose(opp_filtered_df) 
print("\nThe list of unique points overall of the intersection_coordinates_list is: ")
triangulation.print_intersection_coordinates(intersection_coordinates_list_opp) 
m_oppose.save(properties.passed_filepath_tmaps+"m_oppose.html")
print("\n")
print("The oppose approach is done and the map has been saved to m_oppose.html")
print("\n\n")

#Function 6
print("All maps started below")
intersection_coordinates_list_parallel = []
intersection_df_yellow_parallel = pd.DataFrame(columns=['latitude', 'longitude'])
m_parallel, intersection_df_yellow_parallel = triangulation.calculate_triangle_and_intersection(par_filtered_df, intersection_coordinates_list_parallel, intersection_df_yellow_parallel)
intersection_coordinates_list_opposite = []
intersection_df_yellow_opposite = pd.DataFrame(columns=['latitude', 'longitude'])
m_opposite, intersection_df_yellow_opposite = triangulation.calculate_triangle_and_intersection(opp_filtered_df, intersection_coordinates_list_opposite, intersection_df_yellow_opposite)
combined_df = pd.concat([intersection_df_yellow_parallel, intersection_df_yellow_opposite], ignore_index=True)
all_df = pd.concat([par_filtered_df, opp_filtered_df], ignore_index=True) 
#Below will print the coordinates of the intersection areas of the 2 approaches forming together combined_df
#all_df is the dataset that has pairs of triangulation whether from parallel or opposite approach
print("Shape of intersection_df_yellow_parallel:", intersection_df_yellow_parallel.shape)
print("\n Shape of intersection_df_yellow_opposite:", intersection_df_yellow_opposite.shape)
print("\n Shape of combined_df:", combined_df.shape)
print("\n The head of combined_df:", combined_df.head())
print("\n The shape of all_df:", all_df.shape)
print("\n The head of all_df:", all_df.head())
intersection_coordinates_list = []
intersection_df = pd.DataFrame(columns=['latitude', 'longitude'])
all_maps_m_combined = folium.Map(location=[all_df['location_latitude'].iloc[0], all_df['location_longitude'].iloc[0]], zoom_start=15)
intersection_df = triangulation.process_map_rows(all_df, 0.5, intersection_df, all_maps_m_combined)
triangulation.process_pairs_for_second_map(all_df, all_maps_m_combined)
# Display the combined map
#all_df lat and long + centroid and vertex
all_maps_m_combined.save(properties.passed_filepath_tmaps+"all_maps_m_combined.html")
print("All maps map has been saved as 'all_maps_m_combined.html'")
print("\n\n")

#Function 7
df_without_bts_triangulation = triangulation.filter_bts_triangulation_records(processed_data, par_filtered_df, opp_filtered_df)
print("DataFrame of all BTS that do not undergo the Triangulation Records:" ,df_without_bts_triangulation.head())
print("\nShape of the DataFrame all BTS records that do not undergo Triangulation Records:", df_without_bts_triangulation.shape)
BTS_wo_triangulation_map= triangulation.create_folium_map_wo_trianagulation(df_without_bts_triangulation, triangulation.calculate_sector_triangle)
BTS_wo_triangulation_map.save("BTS_wo_triangulation.html")
print("BTS_wo_any_any_triangulation has been saved as 'BTS_wo_triangulation.html'")
print("\n\n")


#Function 8- extra
# Get the missing BTS cell names
missing_bts_cells = triangulation.get_missing_bts_cells(processed_data, df_without_bts_triangulation)
print("Missing BTS Cell Names:", missing_bts_cells)
#print(", ".join(missing_bts_cells)
#all bts that did triangulation and been removed from the map
missing_bts_map = triangulation.create_folium_map_missing_bts(processed_data, missing_bts_cells, triangulation.calculate_sector_triangle)
missing_bts_map.save(properties.passed_filepath_tmaps+"missing_bts_map.html")
print("\n")
print("missing_bts_map has been saved as 'missing_bts_map.html'")
print("\n\n")

#Function 9
final_map_with_vertex_and_centroid = triangulation.create_folium_map_combined(all_df,df_without_bts_triangulation, triangulation.calculate_sector_triangle, distance_km=0.5)
final_map_with_vertex_and_centroid.save("final_map_with_vertex_and_centroid.html")
#all_bts expect the ones that did triangulation (that have been already removed from the map) + yellow areas + centroids and vertex
print(properties.passed_filepath_tmaps+f"final_map_with_vertex_and_centroid saved to: final_map_with_vertex_and_centroid")
print("\n\n")

#Function 10- final
#all_bts expect the ones that did triangulation (that have been already removed from the map) + yellow areas
final_map = triangulation.final_create_combined_map(all_df, df_without_bts_triangulation, triangulation.calculate_sector_triangle)
final_map.save(properties.passed_filepath_tmaps+"final_map.html")
print(f"final_map Map saved to: 'final_map.html'")


