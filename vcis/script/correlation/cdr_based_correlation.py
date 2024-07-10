import pandas as pd
from vcis.utils.utils import CDR_Utils, CDR_Properties
from vcis.correlation.correlation_main import CorrelationMain
import math

##########################################################################################################

imsi_id = '121415223435890'
device_id = '0000000000000000'
check_device_id = ''

start_date = "2023-01-01"
end_date = "2024-01-01"
distance = 200

server = '10.1.10.110'
start_date = str(start_date)
end_date = str(end_date)
##########################################################################################################
utils = CDR_Utils(verbose=True)
properties = CDR_Properties()
correlation_main = CorrelationMain()

start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(start_date)

local = None
distance =  500

##########################################################################################################

table = correlation_main.cdr_correlation(imsi_id=imsi_id,start_date=start_date,end_date=end_date,local=local,distance=distance,server=server)
print(table)

