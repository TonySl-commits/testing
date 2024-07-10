from vcis.utils.utils import CDR_Utils, CDR_Properties

import time
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.cotraveler.cotraveler_main import CotravelerMain
from vcis.reporting.report_main.report_generator import ReportGenerator
from vcis.ai.tools.vcis_tools import vcisTools
##########################################################################################################

utils = CDR_Utils(verbose=True)
properties = CDR_Properties()
oracle_tools = OracleTools()
cotraveler_main = CotravelerMain()
geo_report = ReportGenerator(verbose=True)
vcis_tools = vcisTools()
##########################################################################################################

# CC0B4054-CDD5-4162-9165-237E66696C98
# 3355f290-11b9-41ab-aaad-3d7668007e98
device_id_list = ["93bd82a3-e675-4076-be43-87f0f88ee94d"]
local = False
cotraveler_distance = 100
correlation_distance = 30

start_date = "2023-10-01"
end_date = "2024-01-30"
server = '10.1.10.110'

region = 142
sub_region = 145
table_id=128
##########################################################################################################

start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(start_date)

start_date = int(start_date)
end_date = int(end_date)
##########################################################################################################

start_time_test = time.time()
# # table = pd.read_csv('table.csv')

table, df_merged_list, df_device, df_common, df_history, cotraveler_distance, heatmap_plots, cotraveler_barchart,cotraveler_description= vcis_tools.get_everything(device_id_list=device_id_list,
                start_date=start_date,
                end_date=end_date,
                local=local,
                cotraveler_distance=cotraveler_distance,
                correlation_distance = correlation_distance,
                server=server,
                region=region,
                sub_region = sub_region,table_id=table_id)



# hotspot
# what times they meet
# common_places
# common_places graphs