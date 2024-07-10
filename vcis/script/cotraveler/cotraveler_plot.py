from vcis.utils.utils import CDR_Utils
from vcis.correlation.cdr_correlation_functions import CDRCorrelationFunctions
from vcis.plots.plots_tools import PlotsTools


##########################################################################################################

utils = CDR_Utils()
plots_tools = PlotsTools()
##########################################################################################################


geo_id = '993f1908-2310-4ad8-a289-a2dab1013997'
geo_id_cotraveler = '0B5F4207-D25F-4BC2-AF3D-D29923A72AE0'
start_date = '2021-7-18'
end_date = '2024-1-18'
server = '10.1.10.110'

correlation_functions = CDRCorrelationFunctions()
utils = CDR_Utils(verbose=True)

start_date =  utils.convert_datetime_to_ms(start_date)
end_date =  utils.convert_datetime_to_ms(end_date)
##########################################################################################################

plots_tools.get_cotraveler_plot_folium(geo_id= geo_id , geo_id_cotraveler= geo_id_cotraveler ,start_date= start_date , end_date= end_date,server= server)
##########################################################################################################


