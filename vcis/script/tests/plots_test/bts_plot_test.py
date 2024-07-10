from vcis.utils.utils import CDR_Utils, CDR_Properties
import folium
from vcis.correlation.cdr_correlation_functions import CDRCorrelationFunctions
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools

utils = CDR_Utils()
cassandra_spark_tools = CassandraSparkTools()

start_date = '2020-06-18'
end_date = '2023-06-18'
server = '10.1.10.66'
start_date = str(start_date)
end_date = str(end_date)

##########################################################################################################

correlation_functions = CDRCorrelationFunctions()
utils = CDR_Utils(verbose=True)
properties = CDR_Properties()

bts_table = cassandra_spark_tools.get_spark_data(properties.bts_table_name,passed_connection_host = server)
bts_table = bts_table.toPandas()
bts_table['location_azimuth'] = bts_table['location_azimuth'].replace(['Indoor','indoor','INDOOR'],0).astype(int)
bts_table['location_latitude'] = bts_table['location_latitude'].astype(float)
bts_table['location_longitude'] = bts_table['location_longitude'].astype(float)
start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(end_date)

for index , row in bts_table.iterrows():
    m = folium.Map(location=[bts_table['location_latitude'][0],bts_table['location_longitude'][0]], zoom_start=15)
    # df_geo = self.convert_ms_to_datetime(df_geo)

    latitude = row['location_latitude']
    longitude = row['location_longitude']
    azimuth=int(row['location_azimuth'])
    triangle_coordinates = utils.calculate_sector_triangle(latitude, longitude, azimuth)
    folium.Polygon(locations=triangle_coordinates, color='green', fill=True, fill_color='green', fill_opacity=0.1).add_to(m)
    folium.CircleMarker([latitude, longitude],radius=1,color='red').add_to(m)
    

m.save(properties.passed_filepath_gpx_map + "a.html")