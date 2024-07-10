from vcis.utils.utils import CDR_Utils, CDR_Properties
from vcis.event_tracing.activity_scan_main import ActivityScanMain
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.event_tracing.event_tracing_functions import EventTracingFunctions

class EventTracingMain():
    def __init__(self,verbose=False):
        self.verbose = verbose
        self.utils = CDR_Utils(verbose=verbose)
        self.properties = CDR_Properties()
        self.asc = ActivityScanMain(verbose=verbose)
        self.cassanndra_tools = CassandraTools(verbose=verbose)
        self.event_tracing_functions = EventTracingFunctions(verbose=verbose)

    def event_tracing(self,df_history,distance,session,region,sub_region):
        df_history = self.utils.binning(df_history,distance)
        full_common_df = self.asc.activity_scan_main(df_history,distance,session,region,sub_region)
        full_common_df = self.utils.binning(full_common_df,distance)

        df_common_dict = self.event_tracing_functions.filter_common_df(full_common_df,df_history,distance)

        return df_common_dict

