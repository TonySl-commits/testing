
from vcis.reverse_geocoder.reverse_geocoder_polygon import ReverseGeocoder
from vcis.databases.oracle.oracle_tools import OracleTools
oracle_tool = OracleTools()
gdf = oracle_tool.get_all_cities_geopandas()