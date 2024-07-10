import ast
import numpy as np
from bokeh.io import output_file, save
from bokeh.plotting import figure, show
from bokeh.models import CustomJS, DatetimeTickFormatter, HoverTool, ColumnDataSource, FixedTicker
from bokeh.palettes import Category20
from bokeh.layouts import Spacer, layout
from bokeh.models import MultiSelect
from bokeh.layouts import column, row
import plotly.graph_objects as go
import os
from bs4 import BeautifulSoup

from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.databases.cassandra.cassandra_tools import CassandraTools
class TimeLineFunctions:
    def __init__(self, verbose=False):
        self.properties = CDR_Properties()
        self.utils = CDR_Utils(verbose=True)
        self.oracle_tools=OracleTools()# Convert timestamp to datetime
        self.oracle_tools = OracleTools() 
        self.verbose = verbose

    def timeline(self,table_id):
            # Convert timestamp to datetime
            marker_positions=self.oracle_tools.execute_sql_query(Simulation_id=table_id)
            marker_positions=ast.literal_eval(marker_positions)

            # Extract unique device IDs for y-axis labels 
            device_ids = list(set(pos[3] for pos in marker_positions))

            # make an inverse device id for the multiselect to show the respective lines from up to down or down to up
            inverted_device_ids = list(reversed(device_ids))

            #assigning them colors
            colors = Category20[20][:len(device_ids)]

            # Modify the y_positions dictionary to store only numerical y-axis positions, satrting from 1 to avoid a half point under
            y_positions = {device_id: i+1 for i, device_id in enumerate(device_ids)}
            # print('opppoop',y_positions)
            # Create a ColumnDataSource for Bokeh with a dictionary inside of it to be used
            source = ColumnDataSource(data=dict(
                x=[pos[2]for pos in marker_positions],
                y=[y_positions[pos[3]] for pos in marker_positions],
                label=[pos[3] for pos in marker_positions],
                color=[colors[y_positions[pos[3]] % len(colors)] for pos in marker_positions]
            ))

            # Create the plot
            p = figure(title="Timeline Plot", tools="crosshair, pan, xpan, ypan, reset, wheel_zoom, box_zoom",
                    x_axis_type="datetime", x_axis_label="Date", y_axis_label="Device ID", height=400, width=900)
            p.toolbar.logo = None
            scatter = p.scatter(x='x', y='y', size=8, color='color', source=source)

            # Add HoverTool for additional information on hover
            hover = HoverTool()
            #how the hover should look
            hover.tooltips = [("Device Name", "@label"), ("Time", "@x{%F at %T}")]
            # From millisecond to date in the hover
            hover.formatters = {'@x': 'datetime'}
            p.add_tools(hover)

            # Changing the X axis's date visualization
            formatter = DatetimeTickFormatter(days="%B %d", hours="%Hh:%Mm", minutes="%Mm:%Ss", seconds="%Mm:%Ss")
            p.xaxis.formatter = formatter

            # Formatting plot, tickers help you customize the appearance of the axis by allowing you to specify where ticks should be
            # placed and what labels they should have.
            ticker = FixedTicker(ticks=list(y_positions.values()))
            p.yaxis.ticker = ticker

            #this places the needed device id to its respective tick
            p.yaxis.major_label_overrides = {i: device_id for device_id, i in y_positions.items()}

            # Create a MultiSelect widget
            device_select = MultiSelect(title="Select Device ID:", options=inverted_device_ids, value=inverted_device_ids, height=350)

            # Callback function to update plot based on selected devices
            callback = CustomJS(args=dict(source=source, device_select=device_select, markerpositions=marker_positions, y_positions=y_positions, colors=colors), code="""
                var selected_devices = device_select.value;
                
                let finallarr = [];
                

                for (const key in selected_devices) {
                    const filteredArray = markerpositions.filter(item => {
                        return item[4] === selected_devices[key]; // Change this value to the desired one
                    });

                    finallarr = finallarr.concat(filteredArray); // Use concat to combine arrays
                }

                // Create a new source data object
                var newSourceData = {
                    x: [],
                    y: [],
                    label: [],
                    color: []
                };

                // Loop through finallarr and populate the new source data
                for (var i = 0; i < finallarr.length; i++) {
                    newSourceData.x.push(finallarr[i][2]); // Assuming the timestamp is in milliseconds
                    newSourceData.y.push(y_positions[finallarr[i][4]]);
                    newSourceData.label.push(finallarr[i][3]);
                    newSourceData.color.push(colors[y_positions[finallarr[i][4]] % colors.length]);
                }

                // Update the entire data dictionary of the ColumnDataSource
                source.data = newSourceData;

                // Trigger a change event to update the plot
                source.change.emit();
            """)

            # Attach the callback to the MultiSelect widget
            device_select.js_on_change('value', callback)

            # Create layout for the plot and Select widget
            layout = column(
                row(Spacer(width=10),p, Spacer(width=20), device_select),
                width=1200, height=400
            )

            # if you want to name the outputed html code a certain way
            # output_file("timeline_plot.html")

            # Display the plot in an external web browser
            # show(layout)
            
            output_html_path = self.properties.passed_filepath_reports_html + "Timeline.html"
            output_file(output_html_path)
            save(layout, filename=output_html_path)
            # Show the plot and Select widget
            # Extract the file name from the full path
            file_name = os.path.basename(output_html_path)
            cursor, connection= self.oracle_tools.get_oracle_connection()
            body_content = self.process_html_files2(file_name,output_html_path, connection,table_id)
            print(f"HTML file saved successfully: {output_html_path}")
            return body_content

    def process_html_files2(self,file_name, html_file_path, connection,table_id):
            # Check if the provided path is a directory
            if os.path.isdir(html_file_path):
                for filename in os.listdir(html_file_path):
                    if filename.endswith(".html"):
                        html_path = os.path.join(html_file_path, filename)
                        body_content = self.extract_body(html_path)
                        # body_content=json.dump(body_content)
                        self.insert_into_database(file_name,body_content, connection,table_id)
            elif os.path.isfile(html_file_path) and html_file_path.endswith(".html"):
                # If the provided path is a file and ends with ".html"
                body_content = self.extract_body(html_file_path)
                # body_content=json.dump(body_content)
                self.insert_into_database(file_name,body_content, connection,table_id)
            else:
                print(f"Invalid input: {html_file_path} is not a valid directory or HTML file.")
            return body_content
    def extract_body(self, html_path):
            with open(html_path, 'r', encoding='utf-8') as file:
                soup = BeautifulSoup(file, 'html.parser')
                
                body_tag = soup.body
                if body_tag:
                    body_content = body_tag.prettify()  # Use .prettify() to get the formatted HTML
                    return body_content
                else:
                    return None
                

    def insert_into_database(self,file_name, script_contents, connection,table_id):
            cursor = connection.cursor()
            print(f'ssdx_tmp.tmp_Analytics_Report_{table_id}')
            # Insert the body content and file name into the PL/SQL table
            try:
                self.oracle_tools.drop_create_table(table_name=self.properties.oracle_analytcis_report_table,table_id=table_id,table_schema_query=self.properties._oracle_table_schema_query_analytics_report,drop=False)
            except:
                 pass
            cursor.execute(f"INSERT INTO ssdx_tmp.tmp_Analytics_Report_{table_id} (TYPE_PLOT, HTML_FILE) VALUES (:file_name, :body)",
            {'file_name': file_name, 'body': script_contents})
                

            # Commit changes
            connection.commit()





