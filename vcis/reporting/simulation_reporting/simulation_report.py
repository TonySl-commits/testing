import pandas as pd
import warnings
warnings.filterwarnings('ignore')

from vcis.reporting.simulation_reporting.simulation_report_functions import SimulationReportFunctions

##########################################################################################################


filepath = '/u01/jupyter-scripts/Chris.y/CDR_trace/Trace/src/cdr_trace/reporting/simulation_reporting/report.csv'

simulation_report_generator = SimulationReportFunctions()

data = simulation_report_generator.read_data(filepath)

number_of_hits_per_dow = simulation_report_generator.number_of_hits_per_dow(data)

number_of_hits_per_month = simulation_report_generator.number_of_hits_per_month(data)

number_of_hits_per_hod = simulation_report_generator.number_of_hits_per_hod(data)

summary_statistics = simulation_report_generator.summary_statistics(data)