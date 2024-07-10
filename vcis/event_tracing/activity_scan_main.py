from vcis.event_tracing.activity_scan_functions import ActivityScanFunctions
from vcis.utils.utils import CDR_Utils, CDR_Properties
import math


class ActivityScanMain(ActivityScanFunctions):
    def __init__(self,verbose=False):
        self.utils = CDR_Utils()
        self.properties = CDR_Properties()
        super().__init__(verbose)


    def activity_scan_main(self,df_history,distance,session,region,sub_region):
        df_filtered = df_history.drop_duplicates(subset=['latitude_grid','longitude_grid'])
        df_filtered = df_filtered.sort_values(['latitude_grid','longitude_grid'])
        unique_grids = df_filtered[['latitude_grid', 'longitude_grid']].to_dict('records')

        step_lat,step_lon = self.utils.get_step_size(distance*math.sqrt(2))

        neighboring_grids= self.find_neighboring_grids(unique_grids, step_lat, step_lon)

        polygons_list = self.get_polygon_list(neighboring_grids, step_lat)

        group_grid_df= self.convert_polygon_to_df(neighboring_grids)

        full_min_max_df = self.get_min_max_df(group_grid_df,df_history)
        groups_min_max_df = self.get_groups_min_max_df(full_min_max_df,polygons_list)

        groups_min_max_df['polygon'] = polygons_list

        result = self.activity_scan(groups_min_max_df,session,region=region,sub_region=sub_region)
        return result

