from vcis.cotraveler.cotraveler_functions import CotravelerFunctions
from vcis.plots.cotraveler_plots import CotravelerPlots
from vcis.utils.utils import CDR_Utils,CDR_Properties
import plotly.graph_objs as go
import pandas as pd
import time
import plotly.express as px

import plotly.graph_objects as go
import ast
import plotly.io as pio
import numpy as np
from scipy.stats import gaussian_kde
import matplotlib as mpl
from matplotlib.colors import LinearSegmentedColormap
from vcis.threat_analysis.threat_analysis_functions import ThreatAnalysisFunctions
from plotly.subplots import make_subplots
from vcis.databases.cassandra.cassandra_tools import CassandraTools

class CommonDevicesFunctions:
    def __init__(self,verbose=False):
        self.cotraveler_functions = CotravelerFunctions(verbose=verbose)
        self.cotraveler_plots = CotravelerPlots(verbose=verbose)
        self.utils = CDR_Utils(verbose=verbose)
        self.threat_analysis = ThreatAnalysisFunctions(verbose=verbose)
        self.cassandra_tools = CassandraTools(verbose=verbose)
    # def merge_per_device(self,df_history,table, tol = 15,step_lat=None,step_lon=None):

    #     table.columns = table.columns.str.lower()
    #     table = table[['device_id', 'location_latitude', 'location_longitude',
    #     'usage_timeframe', 'location_name', 'service_provider_id']]

    #     # df_history = pd.concat([table,df_history])
    #     # df_history = df_history.drop_duplicates(keep=False)

    #     device_ids = df_history['device_id'].unique()
    #     df_commons=[]
    #     for i, device_id in enumerate(device_ids):
    #         df_device = df_history[df_history['device_id'] == device_id]
    #         df_device = df_device.sort_values('usage_timeframe')
    #         df_common = pd.DataFrame(columns = ['device_id', 'location_latitude', 'location_longitude', 'usage_timeframe'])
    #         for j, device_id_co in enumerate(device_ids):
    #             if device_id_co != device_id:
    #                 df_device_co = df_history[df_history['device_id'] == device_id_co]
    #                 df_device_co = df_device_co.sort_values('usage_timeframe') 
    #                 df_merged = pd.merge_asof(left=df_device,right=df_device_co,on='usage_timeframe',direction='nearest', tolerance=tol).dropna()
    #                 df_device_co = df_merged[['device_id_y','location_latitude_y','location_longitude_y','usage_timeframe','grid_x','grid_y']]
    #                 df_device_co = df_device_co[df_device_co['grid_x'] == df_device_co['grid_y']]
    #                 df_device_co = df_device_co.drop(['grid_x'],axis=1)
    #                 if df_device_co.empty:
    #                     continue
    #                 print(f"{device_id}  and {device_id_co}")
    #                 df_device_co = df_device_co[['device_id_y','location_latitude_y','location_longitude_y','usage_timeframe','grid_y']]
    #                 df_device_co.columns = df_device_co.columns.str.replace('_y', '')
    #                 df_device_co_visits = self.utils.get_visit_df(df = df_device_co,step_lat=step_lat,step_lon=step_lon)
    #                 # df_device_co_visits = utils.convert_datetime_to_ms(df_device_co_visits)
    #                 df_device_co_visits['duration'] = (df_device_co_visits['end_time'] - df_device_co_visits['start_time']).dt.total_seconds() / 3600
    #                 total_duration = df_device_co_visits['duration'].sum()
    #                 if total_duration <0.01:
    #                     print("skipped")
    #                     continue

    #                 df_common = pd.concat([df_common, df_device_co], ignore_index=True)
    #                 df_common['main_id'] = device_id
    #                 df_common['service_provider_id'] = 9
    #                 df_common['location_name'] = df_common['main_id'].iloc[0]
    #             df_commons.append(df_common)
                    
    #         # unique_device_ids = df_common['device_id'].unique()
    #         # df_common_dict[device_id] = unique_device_ids
    #     return pd.concat(df_commons)

    def merge_per_device(self,df_history,table, tol = 15,step_lat=None,step_lon=None):

        table.columns = table.columns.str.lower()
        table = table[['device_id', 'location_latitude', 'location_longitude',
        'usage_timeframe', 'location_name', 'service_provider_id']]

        # df_history = pd.concat([table,df_history])
        # df_history = df_history.drop_duplicates(keep=False)

        device_ids = df_history['device_id'].unique()
        df_commons=[]
        total =  (len(device_ids) * (len(device_ids)+1))/2
        print(f"★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {total} left ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★")
        for i, device_id in enumerate(device_ids):
            df_device = df_history[df_history['device_id'] == device_id]
            df_device = df_device.sort_values('usage_timeframe')
            df_common = pd.DataFrame(columns = ['device_id', 'location_latitude', 'location_longitude', 'usage_timeframe'])
            for j, device_id_co in enumerate(device_ids):
                if device_id_co != device_id:
                    df_device_co = df_history[df_history['device_id'] == device_id_co]
                    df_device_co = df_device_co.sort_values('usage_timeframe') 
                    df_merged = pd.merge_asof(left=df_device,right=df_device_co,on='usage_timeframe',by='grid',direction='nearest', tolerance=tol).dropna()
                    df_device_co = df_merged[['device_id_y','location_latitude_y','location_longitude_y','usage_timeframe','grid']]
                    
                    # df_device_co = df_device_co[df_device_co['grid_x'] == df_device_co['grid_y']]
                    # df_device_co = df_device_co.drop(['grid_x'],axis=1)
                    if df_device_co.empty:
                        total = total - 1
                        print(f"★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {total} left ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★")
                        continue
                    df_device_co = df_device_co[['device_id_y','location_latitude_y','location_longitude_y','usage_timeframe','grid']]
                    df_device_co.columns = df_device_co.columns.str.replace('_y', '')
                    df_device_co_visits = self.utils.get_visit_df(df = df_device_co,step_lat=step_lat,step_lon=step_lon)
                    # df_device_co_visits = utils.convert_datetime_to_ms(df_device_co_visits)
                    df_device_co_visits['duration'] = (df_device_co_visits['end_time'] - df_device_co_visits['start_time']).dt.total_seconds() / 3600
                    total_duration = df_device_co_visits['duration'].sum()
                    if total_duration <0.01:
                        total = total - 1
                        print("skipped")
                        continue

                    df_common = pd.concat([df_common, df_device_co], ignore_index=True)
                    df_common['main_id'] = device_id
                    df_common['service_provider_id'] = 9
                    df_common['location_name'] = df_common['main_id'].iloc[0]
                df_commons.append(df_common)
                total = total - 1
                print(f"★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ {total} left ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★")
            device_ids = device_ids[device_ids != device_id]
            # unique_device_ids = df_common['device_id'].unique()
            # df_common_dict[device_id] = unique_device_ids
        return pd.concat(df_commons)
    def get_stat_table(self,df_common):
            table = [
                    ['Number of Groups', df_common['main_id'].nunique()],
                    ['Total Hits', df_common.shape[0]],
                    ['Start Date', self.utils.convert_ms_to_datetime_value(df_common['usage_timeframe'].min()).strftime("%Y-%m-%d")],
                    ['End Date',self.utils.convert_ms_to_datetime_value(df_common['usage_timeframe'].max()).strftime("%Y-%m-%d")],
                    ['Countries',  ', '.join(self.utils.add_reverseGeocode_columns(df_common)['country'].unique())]
            ]
            data_dicts = [{'Statistic': row[0], 'Data': row[1]} for row in table]
            summary_table = pd.DataFrame(data_dicts)
            return summary_table
    

    def get_barchart_plots(self,df_main,df_common):
        step_lat,step_lon = self.cotraveler_functions.utils.get_step_size(50)
        df_visits_main = self.utils.get_visit_df(df=df_main,step_lat=step_lat,step_lon = step_lon)
        df_common = self.utils.convert_ms_to_datetime(df_common)
        df_common = df_common.sort_values(['device_id','usage_timeframe'])
        df_visits_cotraveler = pd.concat([self.utils.get_visit_df(df=group.reset_index(drop=True),step_lat=step_lat,step_lon=step_lon) for _, group in df_common.groupby('device_id')])
        
        if not df_visits_cotraveler.empty:
            df_visits_main = self.cotraveler_functions.add_days_list(df_visits_main)
            df_visits_cotraveler =  self.cotraveler_functions.add_days_list(df_visits_cotraveler)
        else:
            return go.Figure()

        date_counts_main,date_counts_cotraveler = self.cotraveler_functions.get_distribution_difference(df_visits_main,df_visits_cotraveler)
        
        distribution_barchart = self.cotraveler_plots.get_barchart(date_counts_main,date_counts_cotraveler)

        return distribution_barchart


    def get_graphs_for_a_group(self, df_history,df_common,ids_in_group):
        group_df = df_history[df_history['device_id'].isin(ids_in_group)]
        df_devices = {}
        df_devices_common = {}
        df_devices_history = {}
        for id in ids_in_group:
            df_device = df_history[df_history['device_id'] == id]
            df_devices[id] = df_device
            df_device_common = df_common[df_common['device_id'] == id]
            df_device_history = df_history[df_history['device_id'] == id]
            df_devices_history[id] = df_device_history
        fig = self.hit_distribution(*df_devices_history.values())
        bar_fig = self.hit_bar(*df_devices_history.values())

        map_fig = px.scatter_mapbox(
            group_df,
            lat="location_latitude",
            lon="location_longitude",
            color="device_id_s",
            size_max=15,
            zoom=1,
            mapbox_style="open-street-map",
            title="Device Locations"
        )
        return [fig,bar_fig,map_fig],group_df
    
    def pie_chart(self, *dfs):
        total_time = 0
        labels = []
        values = []
        custom_colors = ["#b30000", "#7c1158", "#4421af", "#1a53ff"]  # Define your custom colors here

        for df in dfs:
            df = df.reset_index()
            start_time = df['usage_timeframe'].min()
            end_time = df['usage_timeframe'].max()
            time_diff = end_time - start_time
            total_time += time_diff

            labels.append(df['device_id'][0])
            values.append(time_diff)

        values = [value / total_time for value in values]

        fig = go.Figure(data=[go.Pie(labels=labels, values=values, marker=dict(colors=custom_colors), insidetextfont=dict(size=30, family='Roboto', color='white'))])
        fig.update_layout(title='Time Dominance')
        return fig

    # def bar_chart(*dfs):
    #     colors = ['#b30000', '#7c1158', '#4421af', '#1a53ff', '#00b300']  
    #     fig = None

    #     for i, df in enumerate(dfs):
    #         df['usage_timeframe'] = pd.to_datetime(df['usage_timeframe'], unit='ms')
    #         color = colors[i % len(colors)]
    #         opacity = 1 / len(dfs) if len(dfs) > 1 else 0.9  # Adjust opacity based on the number of dfs
    #         trace = px.histogram(df, x='usage_timeframe', nbins=50, color_discrete_sequence=[color]).data[0]
    #         trace.marker.opacity = opacity
    #         if fig is None:
    #             fig = px.histogram(df, x='usage_timeframe', nbins=50, title='Hit Distribution', color_discrete_sequence=[color])
    #             fig.data[0].marker.opacity = opacity
    #         else:
    #             fig.add_trace(trace)

    #     fig.update_layout(
    #         title='Hit Distribution with KDE',
    #         xaxis_title='Usage Timeframe',
    #         yaxis_title='Density',
    #         font=dict(family='Roboto, sans-serif', size=14),
    #         barmode='overlay'
    #     )

    #     fig.update_traces(marker=dict(line=dict(color='rgba(255,49,42,0)', width=1)))

    #     return fig

    def hit_distribution(self, *dfs):
        dow_color_palette = ['#003366', '#006699', '#33cccc', '#00ccff', '#0099cc', '#336699', '#3366cc', '#0066cc', '#0099ff', '#0066ff', '#0000cc', '#9966ff']
        custom_cmap = LinearSegmentedColormap.from_list("custom_cmap", dow_color_palette)

        cmap = custom_cmap.resampled(len(dfs))
        colors = cmap(np.arange(0, cmap.N))

        fig = go.Figure()
        kde_height = 500 / (500 + 25 * len(dfs))
        row_heights = [kde_height] + [(1 - kde_height) / len(dfs)] * len(dfs)

        fig = make_subplots(rows=len(dfs) + 1, cols=1, shared_xaxes=True, vertical_spacing=0, row_heights=row_heights)
        for i, df in enumerate(dfs):
            df = df.reset_index(drop=True)
            df['usage_timeframe'] = pd.to_datetime(df['usage_timeframe'], unit='ms')

            kde = gaussian_kde(df['usage_timeframe'].astype(np.int64), bw_method=0.03)
            x_range = np.linspace(df['usage_timeframe'].astype(np.int64).min(), df['usage_timeframe'].astype(np.int64).max(), 1500)
            kde_values = kde(x_range)

            x_range_datetime = pd.to_datetime(x_range)

            # Add KDE line with shade
            fig.add_trace(go.Scatter(
                x=x_range_datetime,
                y=kde_values,
                mode='lines',
                name=df['device_id_s'][0],
                legendgroup=df['device_id_s'][0],  # Add legendgroup
                fill='tozeroy',
                fillcolor=f'rgba({colors[i][0]},{colors[i][1]},{colors[i][2]}, 0.3)',
                line=dict(color=f'rgb({colors[i][0]},{colors[i][1]},{colors[i][2]})', width=2.5)
            ), row=1, col=1)

            # Add rug plot
            fig.add_trace(go.Scatter(
                x=df['usage_timeframe'],
                y=[0] * len(df),
                mode='markers',
                marker=dict(color=f'rgb({colors[i][0]},{colors[i][1]},{colors[i][2]})', symbol='line-ns-open', size=10),
                legendgroup=df['device_id'][0],  # Add legendgroup
                showlegend=False  # Hide this trace from the legend
            ), row=i + 2, col=1)
            fig.update_yaxes(showticklabels=False, row=i + 2, col=1)

        fig.update_layout(
            title='Distribution of Hits',
            height=500 + 25 * len(dfs),
            xaxis_title='',
            yaxis_title='',
            yaxis=dict(showticklabels=False),
            plot_bgcolor='rgba(0,0,0,0)',
            showlegend=True,
            legend=dict(x=1, y=1, xanchor='right', yanchor='top', bgcolor='rgba(0,0,0,0)', bordercolor='rgba(0,0,0,0)'),
            margin=dict(l=0, r=0)
        )

        return fig

    def hit_bar(self, *df_devices):
        dow_color_palette = ['#003366', '#006699', '#33cccc', '#00ccff', '#0099cc', '#336699', '#3366cc', '#0066cc', '#0099ff', '#0066ff', '#0000cc', '#9966ff']
        custom_cmap = LinearSegmentedColormap.from_list("custom_cmap", dow_color_palette)

        cmap = custom_cmap.resampled(len(df_devices))
        colors = cmap(np.arange(0, cmap.N))

        # Create bar chart
        fig = go.Figure()

        for i, df_device in enumerate(df_devices):
            count = len(df_device)
            device_label = df_device['device_id_s'].iloc[0]
            
            color_rgb = f'rgb({colors[i][0]},{colors[i][1]},{colors[i][2]})'
            color_rgba = f'rgba({colors[i][0]},{colors[i][1]},{colors[i][2]}, 0.7)'

            fig.add_trace(go.Bar(
                x=[device_label],
                y=[count],
                name=device_label,
                marker=dict(
                    color=color_rgba,
                    line=dict(color=color_rgb, width=2)
                )
            ))

        fig.update_layout(
            title='Count of Records per Device',
            xaxis_title='Device',
            yaxis_title='Count',
            plot_bgcolor='rgba(0,0,0,0)',
            showlegend=True,
            legend=dict(x=1, y=1, xanchor='right', yanchor='top', bgcolor='rgba(255,255,255,0.5)', bordercolor='rgba(0,0,0,0)'),
            margin=dict(l=0, r=0)
        )

        return fig

    def get_groups(self, df_common):
        groups = []
        for main_id in df_common['main_id'].unique():
            devices_list = df_common[df_common['main_id'] == main_id]['device_id'].unique().tolist()
            devices_list.append(main_id)
            groups.append(devices_list)

        def merge_similar_groups(groups, tolerance=0.1):
            """
            Merge groups that are similar based on a tolerance level for device_id comparison.
            """
            merged_groups = []
            while groups:
                base_group = groups.pop(0)
                base_set = set(base_group)
                i = 0
                while i < len(groups):
                    compare_set = set(groups[i])
                    if len(base_set.intersection(compare_set)) / len(base_set.union(compare_set)) > tolerance:
                        base_set.update(compare_set)
                        groups.pop(i)
                    else:
                        i += 1
                merged_groups.append(list(base_set))
            return merged_groups

        # Merge similar groups and create a DataFrame
        merged_groups = merge_similar_groups(groups)
        group_data = []
        for idx, group in enumerate(merged_groups):
            min_time = df_common[df_common['main_id'].isin(group)]['usage_timeframe'].min()
            group_data.append({'group_id': idx,'group_ids': group, 'usage_timeframe': min_time})
            df_common.loc[df_common['main_id'].isin(group), 'group_id'] = idx

        return pd.DataFrame(group_data),df_common


    def get_graphs_for_groups(self,df_history,df_common, groups,session,start_date,end_date,region,sub_region):
        i = 0
        print(groups['device_id'].unique())
        print(duh)
        for group in groups['group_ids']:
            group = ast.literal_eval(group)

            graphs_for_group,group_df = self.get_graphs_for_a_group(df_history,df_common,group)
            fig = graphs_for_group[0]
            bar_fig = graphs_for_group[1]
            map_fig = graphs_for_group[2]
            group_df = df_history[df_history['device_id'].isin(group)]
            group_df['usage_timeframe'] = pd.to_datetime(group_df['usage_timeframe'], unit='ms')
            device_id_mapping = {old_id: f"Device_{str(i+1).zfill(3)}" for i, old_id in enumerate(group_df['device_id'].unique())}
            group_df['device_id_s'] = group_df['device_id'].map(device_id_mapping)
            num_unique_devices = group_df['device_id_s'].nunique()
            filtered_df_common = df_common[df_common['group_id'] == i]
            group_id = int(filtered_df_common['group_id'].iloc[0])
            top_3_cities = filtered_df_common['city'].value_counts().head(3)
            most_common_place = top_3_cities.index.tolist()
            most_visited_place = group_df['usage_timeframe'].dt.hour.mode()[0]
            number_of_visited_places = group_df['usage_timeframe'].dt.day_name().mode()[0]
            black_listed_devices = ['ea830a7f-4e1c-42db-8d38-c1404f1b7aff']
            df_blacklist_devices = self.cassandra_tools.get_device_history_geo_chunks(black_listed_devices,session = session, start_date = start_date, end_date = end_date,region=region,sub_region=sub_region)

            devices_threat_table = self.get_threat_table_devices(df_history,groups,df_blacklist_devices, 10000)
            devices_threat_table['device_id_s'] = devices_threat_table['device_id'].map(device_id_mapping)
            df2_exploded = devices_threat_table.explode('Contaced Black Listed Device(s)')
            filtered_devices_threat_table = df2_exploded[df2_exploded['device_id'].isin(group)]
            filtered_devices_threat_table = filtered_devices_threat_table.drop(columns=['Lifespan Info','device_id'])
            filtered_devices_threat_table = filtered_devices_threat_table.rename(columns={'device_id_s': 'Device ID'})
            filtered_devices_threat_table = filtered_devices_threat_table[['Device ID','Sus Areas Visited & Time','Contaced Black Listed Device(s)','First & Last Activity']]
            filtered_devices_threat_table['Sus Areas Visited & Time'] = filtered_devices_threat_table['Sus Areas Visited & Time'].apply(lambda x: 'No Suspicious areas' if not x else x)
            filtered_devices_threat_table['Contaced Black Listed Device(s)'] = filtered_devices_threat_table['Contaced Black Listed Device(s)'].fillna('No blacklisted devices in contact')

            # Generate HTML table from filtered_devices_threat_table
            table_html = filtered_devices_threat_table.to_html(index=False, classes='table table-striped', border=0)
            
            # Generate HTML table for device_id and device_id_s from groups
            reference_table_html = group_df[['device_id', 'device_id_s']].drop_duplicates().to_html(index=False, classes='table table-striped', border=0)
            
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Interactive Dashboard</title>
                <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
                <link href="https://fonts.googleapis.com/css2?family=Roboto+Condensed:wght@300;400;700&display=swap" rel="stylesheet">
                <style>
                    body {{
                        font-family: 'Roboto Condensed', sans-serif;
                        color: #4267B2;
                        background-color: #FFFFFF;
                    }}
                    .dashboard {{
                        background-color: #FFFFFF;
                        padding: 20px;
                        border-radius: 10px;
                        box-shadow: 0 0 20px rgba(0,0,0,0.1);
                    }}
                    .stat-box {{
                        padding: 10px;
                        background-color: rgba(9, 8, 255, 0.3);
                        border-radius: 5px;
                        color: #000000;
                        font-family: 'Roboto', sans-serif; 
                        border: 2px solid rgba(9, 8, 255, 1);
                        transition: transform 0.3s ease, background-color 0.3s ease;
                    }}
                    .stat-box:hover {{
                        transform: scale(1.05);
                        background-color: rgba(9, 8, 255, 0.4);
                    }}
                    .stat-box h5 {{
                        font-weight: bold; 
                        font-family: 'Roboto Condensed', sans-serif; 
                        font-size: 20px;
                    }}
                    .table-container {{
                        background-color: #F0F2F5;
                        padding: 20px;
                        border-radius: 10px;
                        margin-top: 20px;
                        color: #4267B2;
                        border: 1px solid #4267B2;
                        max-height: 400px;
                        overflow-y: auto;
                    }}
                    table {{
                        width: 100%;
                        border-collapse: collapse;
                    }}
                    th, td {{
                        padding: 10px;
                        text-align: left;
                        border-bottom: 1px solid #4267B2;
                    }}
                    th {{
                        background-color: #F0F2F5 !important; 
                        color: #4267B2;
                        position: sticky;
                        top: 0;
                        z-index: 1;
                    }}
                    body::before {{
                        content: '';
                        position: fixed;
                        top: 0;
                        left: 0;
                        width: 100%;
                        height: 100%;
                        background-color: #FFFFFF;
                        z-index: -1;
                    }}
                    html {{
                        background: #F2F2F2;
                    }}
                    .logo-holder {{
                        width: 180px;
                        height: 220px;
                        position: fixed;
                        top: -70px; /* the more you increase the more it goes up  */
                        right: 200px; /* the more you increase the more it goes to the left  */
                        padding-top: 0px;
                        padding-bottom: 24px;
                        -webkit-transform: rotate(-90deg); 
                        transform: rotate(-90deg); 
                        background: none; 
                        transform-origin: right top;
                    }}

                    .bg {{
                        position: absolute;
                        top: 9px;
                        left: 8px;
                        right: 8px;
                        bottom: 8px;
                        background: url(http://boutique.flarework.com/wp-content/themes/boutique/img/logo_large.png) center 0px no-repeat;
                        background-size: contain;
                        -webkit-filter: drop-shadow(0px 6px 25px rgba(0,0,0,0.5));
                    }}

                    .bar {{
                        position: relative;
                        height: 8px;
                        background: #6a6a6a;
                        width: 0%;
                        top: 0px;
                        left: 18px;
                        margin-top: 8px;
                        box-shadow: 0 0 3px rgba(192,192,192,0.9);
                        animation: fill 5s infinite alternate, colors 5s infinite alternate;
                    }}
                    .bar.fill1 {{
                        animation: fill-1 5s infinite alternate, colors 5s infinite alternate;
                    }}
                    .bar.fill2 {{
                        animation:  fill-2 5s infinite alternate, colors 5s infinite alternate;
                    }}
                    .bar.fill3 {{
                        animation:  fill-3 5s infinite alternate, colors 5s infinite alternate;
                    }}
                    .bar.fill4 {{
                        animation:  fill-4 5s infinite alternate, colors 5s infinite alternate;
                    }}
                    .bar.fill5 {{
                        animation:  fill-5 5s infinite alternate, colors 5s infinite alternate;
                    }}
                    .bar.fill6 {{
                        animation:  fill-6 5s infinite alternate, colors 5s infinite alternate;
                    }}
                    @keyframes fill {{
                        0%   {{ width: 0; }}
                        33%  {{ width: 30px;}}
                        66%  {{ width: 10px;}}
                        100% {{ width: 105px; }}
                    }}
                    @keyframes fill-1 {{
                        0%   {{ width: 0; }}
                        33%  {{ width: 50px;}}
                        66%  {{ width: 20px;}}
                        100% {{ width: 130px; }}
                    }}
                    @keyframes fill-2 {{
                        0%   {{ width: 0; }}
                        33%  {{ width: 90px;}}
                        66%  {{ width: 70px;}}
                        100% {{ width: 136px; }}
                    }}
                    @keyframes fill-3 {{
                        0%   {{ width: 0; }}
                        33%  {{ width: 50px;}}
                        66%  {{ width: 24px;}}
                        100% {{ width: 109px; }}
                    }}
                    @keyframes fill-4 {{
                        0%   {{ width: 0; }}
                        33%  {{ width: 98px;}}
                        66%  {{ width: 34px;}}
                        100% {{ width: 99px; }}
                    }}
                    @keyframes fill-5 {{
                        0%   {{ width: 0; }}
                        33%  {{ width: 30px;}}
                        66%  {{ width: 10px;}}
                        100% {{ width: 148px; }}
                    }}
                    @keyframes fill-6 {{
                        0%   {{ width: 0; }}
                        33%  {{ width: 48px;}}
                        66%  {{ width: 22px;}}
                        100% {{ width: 140px; }}
                    }}
                    @keyframes colors {{
                        0% {{ background-color: #003366;}}
                        50% {{ background-color: #33cccc;}}
                        100% {{ background-color: #0099cc;}}
                    }}
                    .reference-table-toggle {{
                        position: fixed;
                        top: 10px;
                        right: 0;
                        background-color: #4267B2;
                        color: white;
                        padding: 10px;
                        border-radius: 5px 0 0 5px;
                        cursor: pointer;
                        z-index: 1000;
                    }}
                    .reference-table-container {{
                        position: fixed;
                        top: 0;
                        right: -300px;
                        width: 300px;
                        height: 100%;
                        background-color: #F0F2F5;
                        border-left: 1px solid #4267B2;
                        overflow-y: auto;
                        transition: right 0.3s ease;
                        z-index: 999;
                    }}
                    .reference-table-container.show {{
                        right: 0;
                    }}
                </style>
                <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            </head>
            <body>
                <div class="container">
                    <div class="dashboard">
                        <h1 class="display-4 text-center mb-4" style="color: rgba(9, 8, 255, 0.7);">CASE INSIGHTS FOR GROUP {group_id}</h1>
                        <div class="row">
                            <div class="col-md-3">
                                <div class="stat-box">
                                    <h5>Number of Unique Devices</h5>
                                    <p>{num_unique_devices}</p>
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="stat-box">
                                    <h5>Most Common Place</h5>
                                    <p>{most_common_place}</p>
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="stat-box">
                                    <h5>Most Visited Place</h5>
                                    <p>{most_visited_place}</p>
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="stat-box">
                                    <h5>Number of Visited Places</h5>
                                    <p>{number_of_visited_places}</p>
                                </div>
                            </div>
                        </div>
                        <div class="row mt-4">
                            <div class="col-md-12">
                                {fig.to_html(full_html=False, include_plotlyjs='cdn')}
                            </div>
                        </div>
                        <div class="row mt-4">
                            <div class="col-md-8">
                                {map_fig.to_html(full_html=False, include_plotlyjs='cdn')}
                            </div>
                            <div class="col-md-4">
                                {bar_fig.to_html(full_html=False, include_plotlyjs='cdn')}
                            </div>
                        </div>
                        <div class="table-container">
                            {table_html}
                        </div>
                    </div>
                </div>
                <div class="reference-table-toggle" onclick="toggleReferenceTable()">&#9664; Reference Table</div>
                <div class="reference-table-container" id="referenceTable">
                    {reference_table_html}
                </div>
                <div class="logo-holder">
                    <div class="bg"></div>
                    <div class="bar fill1"></div>
                    <div class="bar fill2"></div>
                    <div class="bar fill3"></div>
                    <div class="bar fill4"></div>
                    <div class="bar fill5"></div>
                    <div class="bar fill6"></div>
                </div>
                <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
                <script>
                    function toggleReferenceTable() {{
                        var refTable = document.getElementById('referenceTable');
                        refTable.classList.toggle('show');
                    }}
                </script>
            </body>
            </html>
            """

            groups.at[i, 'html_graphs'] = html_content
            with open('combined_graphs.html', 'w', encoding='utf-8') as file:
                file.write(html_content)
            i += 1
        return groups
    

    def get_threat_table_devices(self,df_history,sus_areas,df_blacklist_devices, max_time_gap,distance:int=100):
        df_threat_devices = pd.DataFrame()
        for device_id in df_history['device_id'].unique():
            df_main = df_history[df_history['device_id'] == device_id]
            df_threat = self.threat_analysis.get_threat_analysis_list(df_main, sus_areas, df_blacklist_devices, max_time_gap, distance=distance)
            df_threat_devices = pd.concat([df_threat_devices,df_threat],ignore_index=True)
        df_threat_devices = df_threat_devices.reset_index(drop=True)
        return df_threat_devices

