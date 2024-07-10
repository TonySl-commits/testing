
import pandas as pd
import numpy as np
import folium
import math
import plotly.graph_objects as go
import folium
from datetime import datetime, timedelta, timezone
from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from folium.plugins import HeatMap
import matplotlib.pyplot as plt
import seaborn as sns

class CotravelerPlots():
    def __init__(self, verbose=False):
        self.properties = CDR_Properties()
        self.utils = CDR_Utils(verbose=True)
        self.cassandra_tools = CassandraTools()
        self.cassandra_spark_tools = CassandraSparkTools()
        self.verbose = verbose

    def plot_binning_example(self):
        min_lat, max_lat = 33.88487833176962, 33.8899
        min_lon, max_lon =  35.50547852832974, 35.534660963007276

        rng = np.random.default_rng()
        latitudes = rng.uniform(low=min_lat, high=max_lat, size=1000)
        longitudes = rng.uniform(low=min_lon, high=max_lon, size=1000)

        # Create the DataFrame
        df = pd.DataFrame({'location_latitude': latitudes, 'location_longitude': longitudes})
        df, lat_step, lon_step= self.utils.binning(df, 100,ret=True)

        m = folium.Map(location=[33.88487833176962,35.50547852832974], zoom_start=12)
        #drop duplicates on latitude and longitude
        for i, row in df.drop_duplicates(subset=['latitude_grid', 'longitude_grid']).iterrows():
            north = row['latitude_grid'] + lat_step/2
            south = (row['latitude_grid'])- lat_step/2
            east = row['longitude_grid'] + lon_step/2
            west = row['longitude_grid'] - lon_step/2

            # Draw the rectangle on the map
            folium.Rectangle(
                bounds=[[south, west], [north, east]],
                color='#ff7800',  # Color of the rectangle
                fill=True,
                fill_color='#ff7800',  # Fill color of the rectangle
                fill_opacity=0.5,
                weight=2  # Border thickness
            ).add_to(m)

        for i, row in df.iterrows():
            folium.CircleMarker([row['location_latitude'], row['location_longitude']], radius=3, color='blue', fill=True, fill_color='blue', fill_opacity=0.6).add_to(m)
            folium.CircleMarker([row['latitude_grid'], row['longitude_grid']], radius=3, color='red', fill=True, fill_color='red', fill_opacity=0.6).add_to(m)
        # m.save(self.properties.passed_filepath_cdr_map+'binning_example.html')

    def plot_binning(self,df):
        df, lat_step, lon_step= self.utils.binning(df, 100,ret=True)

        # Create the map
        m = folium.Map(location=[33.88487833176962,35.50547852832974], zoom_start=12)
        #drop duplicates on latitude and longitude
        for i, row in df.drop_duplicates(subset=['latitude_grid', 'longitude_grid']).iterrows():
            north = row['latitude_grid'] + lat_step/2
            south = (row['latitude_grid'])- lat_step/2
            east = row['longitude_grid'] + lon_step/2
            west = row['longitude_grid'] - lon_step/2

            # Draw the rectangle on the map
            folium.Rectangle(
                bounds=[[south, west], [north, east]],
                color='#ff7800',  # Color of the rectangle
                fill=True,
                fill_color='#ff7800',  # Fill color of the rectangle
                fill_opacity=0.5,
                weight=2  # Border thickness
            ).add_to(m)

        for i, row in df.iterrows():
            folium.CircleMarker([row['location_latitude'], row['location_longitude']], radius=3, color='blue', fill=True, fill_color='blue', fill_opacity=0.6).add_to(m)
            folium.CircleMarker([row['latitude_grid'], row['longitude_grid']], radius=3, color='red', fill=True, fill_color='red', fill_opacity=0.6).add_to(m)
        m.save(self.properties.passed_filepath_cdr_map+'binning_device.html')

    def plot_scan_circles(self,df):
        m = folium.Map(location=[33.88487833176962,35.50547852832974], zoom_start=12)
        df, lat_step, lon_step= self.utils.binning(df, 100,ret=True)
        for i, row in df.drop_duplicates(subset=['latitude_grid', 'longitude_grid']).iterrows():
            north = row['latitude_grid'] + lat_step/2
            south = (row['latitude_grid'])- lat_step/2
            east = row['longitude_grid'] + lon_step/2
            west = row['longitude_grid'] - lon_step/2

            # Draw the rectangle on the map
            folium.Rectangle(
                bounds=[[south, west], [north, east]],
                color='#ff7800',  # Color of the rectangle
                fill=True,
                fill_color='#ff7800',  # Fill color of the rectangle
                fill_opacity=0.5,
                weight=2  # Border thickness
            ).add_to(m)
            folium.Circle([row['latitude_grid'], row['longitude_grid']],radius=50*math.sqrt(2), color='red', fill=True, fill_color='red', fill_opacity=0.1).add_to(m)

        for i, row in df.iterrows():
            # folium.CircleMarker([row['location_latitude'], row['location_longitude']], radius=1, color='blue', fill=True, fill_color='blue', fill_opacity=0.6).add_to(m)
            folium.Circle([row['latitude_grid'], row['longitude_grid']], radius=1, color='black', fill=True, fill_color='red', fill_opacity=0.6).add_to(m)
        m.save(self.properties.passed_filepath_cdr_map+'scan_circle.html')

    def plot_common_heatmap(self,df_device_common,df_main,df_device):

        grid_counts = df_device_common.groupby(['grid']).size().reset_index(name='count')
        grid_counts['latitude_grid'],grid_counts['longitude_grid']= grid_counts['grid'].str.split(',').str

        df_heatmap = df_device_common.groupby(['location_latitude','location_longitude']).size().reset_index(name='count')

        # Create a map centered around the mean latitude and longitude of the main device
        m = folium.Map(location=[df_main['location_latitude'].mean(), df_main['location_longitude'].mean()], zoom_start=3 ,max_zoom= 6)

        # Add circles for the main device
        for index, row in df_main.iterrows():
            folium.Circle(
                location=[row['location_latitude'], row['location_longitude']],
                radius=2, # Adjust the radius as needed
                color='black'
            ).add_to(m)

        # Add circles for the cotraveler device
        for index, row in df_device.iterrows():
            folium.Circle(
                location=[row['location_latitude'], row['location_longitude']],
                radius=1, # Adjust the radius as needed
                color='blue'
            ).add_to(m)

        # Define the color gradient
        color_gradient = {
            0: 'green',
            0.5: 'yellow',
            1: 'red'
        }

        # Add a heatmap for common hits
        heat_data = [[row['location_latitude'], row['location_longitude'], row['count']] for index, row in df_heatmap.iterrows()]
        heatmap = HeatMap(heat_data, radius=20)
        heatmap.add_to(m)

        # # Add a custom legend
        # legend_html = """
        # <div style="position: fixed; bottom: 50px; left: 50px; width: 150px; height: 200px; background-color: white; z-index:9999; font-size:14px; border-radius: 5px; box-shadow: 0 0 10px rgba(0,0,0,0.1); padding: 10px;">
        #     <p>Low</p>
        #     <div style="width: 100%; height: 100px; background: linear-gradient(to bottom, #440154, #440154, #31678E, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387, #20A387)"
        # </div>
        # """
        # m.get_root().html.add_child(folium.Element(legend_html))

        # Add layer control
        folium.LayerControl().add_to(m)
        def create_popup_table(row):
            return folium.Popup(
                f"""
                <table>
                    <tr><th>Latitude</th><td>{row['latitude_grid']}</td></tr>
                    <tr><th>Longitude</th><td>{row['longitude_grid']}</td></tr>
                    <tr><th>Grid</th><td>{row['grid']}</td></tr>
                    <tr><th>Count</th><td>{row['count']}</td></tr>
                </table>
                """,
                max_width=250
            )


        for index, row in grid_counts.iterrows():
            folium.Circle(
                location=[row['latitude_grid'], row['longitude_grid']],
                radius=50, # Adjust the radius as needed
                color='blue',
                fill=True,
                fill_color='blue',
                fill_opacity=0,
                opacity=0,
                popup=create_popup_table(row)
            ).add_to(m)

        return m
    
    def get_barchart(self,date_counts_main, date_counts_cotraveler):
        # Separate main device data and other devices data
        main_device_id = date_counts_main['device_id'].iloc[0]
        main_device_data = date_counts_main.set_index('all_dates')['count'].to_dict()
        other_devices_data = date_counts_cotraveler.groupby(['device_id', 'all_dates'])['count'].sum().reset_index()
        # Set the order of days
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

        # Convert 'all_dates' to categorical data type with ordered categories
        other_devices_data['all_dates'] = pd.Categorical(other_devices_data['all_dates'], categories=days_order, ordered=True)
        main_device_data_squence = {k: v for k, v in sorted(main_device_data.items(), key=lambda item: days_order.index(item[0]))}

        # Set the bar width
        num_other_devices = other_devices_data['device_id'].nunique()
        main_bar_width = 0.6
        other_bar_width = main_bar_width / num_other_devices

        # Color palette
        palette = ["#fee090", "#fdae61", "#4575b4", "#313695", "#e0f3f8", "#abd9e9", "#d73027", "#a50026"]

        # Create the trace for the main device bar
        trace_main = go.Bar(
            x=list(main_device_data_squence.keys()),
            y=list(main_device_data_squence.values()),
            width=main_bar_width,
            name=main_device_id,
            marker_color=palette[0]  # Set the color for the main device bar
        )

        # Create traces for other devices bars
        traces_other = []
        i = 0
        print("num_other_devices", num_other_devices)
        if num_other_devices <=8:
            for device_id, group in other_devices_data.groupby('device_id'):
                trace = go.Bar(
                    x=group['all_dates'],
                    y=group['count'],
                    width=other_bar_width,
                    base=0,
                    name=device_id,
                    offset=-main_bar_width/2 + (i + 1) * other_bar_width,
                    marker_color=palette[i + 1]  # Set the color for each other device bar
                )
                traces_other.append(trace)
                i += 1
        else:
            for device_id, group in other_devices_data.groupby('device_id'):
                trace = go.Bar(
                    x=group['all_dates'],
                    y=group['count'],
                    width=other_bar_width,
                    base=0,
                    name=device_id,
                    offset=-main_bar_width/2 + (0) * other_bar_width,
                    marker_color=palette[0]  # Set the color for each other device bar
                )
                traces_other.append(trace)
                i += 1
                if i==9:
                    break

        # Create the layout
        layout = go.Layout(
            barmode='group',
            title='Cotravelers DOW Distribution',
            xaxis=dict(title='Day of Week'),
            yaxis=dict(title='Number of Visits')
        )
        # Create the figure and plot
        fig = go.Figure(data=[trace_main] + traces_other, layout=layout)
        figure_layout = {'paper_bgcolor': 'rgba(255, 255, 255, 1)', 'plot_bgcolor': 'rgba(255, 255, 255, 1)'}
        fig.update_layout(figure_layout)
        return fig