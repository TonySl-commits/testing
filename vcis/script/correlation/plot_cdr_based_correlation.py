from vcis.utils.utils import CDR_Utils
from vcis.plots.plots_tools import PlotsTools
import math

##########################################################################################################

utils = CDR_Utils()
plots_tools = PlotsTools()
##########################################################################################################

geo_id = '8354490f-a78c-4807-9cf4-fc54d46a3190'
imsi_id = '415649034675843'
start_date = '2023-12-18'
end_date = '2024-01-18'
server = '10.1.10.110'


##########################################################################################################

start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(start_date)

##########################################################################################################

plot = plots_tools.get_correlation_plot_folium(geo_id= geo_id , imsi_id= imsi_id ,start_date= start_date , end_date= end_date,server= server)

##########################################################################################################


