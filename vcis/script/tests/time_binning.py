import pandas as pd
from vcis.utils.properties import CDR_Properties

# Display limit none pandas
pd.set_option('display.max_columns', None)

properties = CDR_Properties()

df_main = pd.read_csv(properties.passed_filepath_excel + 'df_main.csv')

# Convert the time column to datetime format if it's not already
df_main['usage_timeframe'] = pd.to_datetime(df_main['usage_timeframe'])

def bin_data(dataframe, start_date, end_date, freq):
    # Define bins
    time_bins = pd.date_range(start=start_date, end=end_date, freq=freq)
    # Bin the data
    dataframe['time_bin'] = pd.cut(dataframe['usage_timeframe'], bins=time_bins)
    return dataframe

# Apply binning function to the main dataframe and a sample of 100 rows
df_main_binned = bin_data(df_main, '1970-01-01', '2050-01-01', '1D')
df_sample_binned = bin_data(df_main.sample(100), '1970-01-01', '2050-01-01', '1D')

# Merge the binned main dataframe and the binned sample
df_merged = pd.merge(df_main_binned, df_sample_binned, how='inner')

print(df_merged.shape)
