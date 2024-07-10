from vcis.reporting_pdf.simulation_reporting.simulation_report_functions import SimulationReportFunctions
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.plots.plots_tools import PlotsTools
import json

table_id = 179121

oracle_tools = OracleTools()
plot_tools = PlotsTools()
simulation_report_functions = SimulationReportFunctions()
query = f"SELECT * FROM locdba.loc_report_config a where a.loc_report_config_id ={table_id}  "
table= oracle_tools.get_oracle_query(query)
report_type = table['LOC_REPORT_TYPE'][0]
table_id = table['LOC_REPORT_CONFIG_ID'][0]
report_name = table['LOC_REPORT_NAME'][0]
start_date = table['FILTER_BDATE'][0]
end_date = table['FILTER_EDATE'][0]
original_table, table , mapping_table = simulation_report_functions.get_simulation_data(table_id=table_id, table=table)
print(table)

shapes = oracle_tools.get_simulation_shapes(table_id=table_id)

print(shapes)

plot_tools.get_simulation_plot_2(data=table, shapes = shapes)