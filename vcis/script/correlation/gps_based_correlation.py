import pandas as pd
from vcis.utils.utils import CDR_Utils, CDR_Properties
from vcis.correlation.correlation_main import CorrelationMain
import math

##########################################################################################################


device_id_list =["93bd82a3-e675-4076-be43-87f0f88ee94d"]
check_device_id = ''

start_date = "2023-07-07"
end_date = "2024-07-07"
distance = 200
verbose = True
server = '10.1.2.205'
start_date = str(start_date)
end_date = str(end_date)
##########################################################################################################
utils = CDR_Utils(verbose=verbose)
properties = CDR_Properties()
correlation_main = CorrelationMain(verbose=verbose)

start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(end_date)

local = None
distance =  30
region = 142
sub_region = 145
##########################################################################################################

top_id = correlation_main.gps_correlation(device_id_list = device_id_list,
                                        start_date = start_date,
                                        end_date = end_date,
                                        local = local,
                                        distance = distance,
                                        server = server,
                                        region = region,
                                        sub_region = sub_region)
print(top_id)

