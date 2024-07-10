import pandas as pd
from vcis.utils.utils import CDR_Utils
from vcis.utils.utils import CDR_Properties
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from datetime import datetime
import string

utils = CDR_Utils()
properties = CDR_Properties()
cassandra_tools = CassandraTools()

device_ids = ['1f3df857-7a97-4ed7-a145-bfd143304158', '03131537-7C0B-4B0B-A3DF-32EAC58327F1', '97349a1c-993a-4f91-a494-3dd2e8215849', 'E558E6A1-C200-4C19-AA20-18E286AEE1AA']

start_date = '2023-01-01'
end_date = datetime.now().strftime("%Y-%m-%d")

region = '142'
sub_region = '145'

server = '10.1.10.110'

start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(end_date)

start_date = int(start_date)
end_date = int(end_date)

# Fetch device history using the CassandraTools get_device_history_geo_chunks function
session = cassandra_tools.get_cassandra_connection(server)
df_device_history = cassandra_tools.get_device_history_geo_chunks(
    device_ids, start_date, end_date, session=session, 
    region=region, sub_region=sub_region
)



import numpy as np

def mask_data(df, mask_percentage=30):
    """ Mask given percentage of data by replacing with NaN """
    mask_count = int(np.ceil(mask_percentage / 100.0 * len(df)))
    mask_indices = np.random.choice(df.index, mask_count, replace=False)
    df = df.drop(mask_indices)
    return df

def generate_new_id(old_id):
    all_chars = string.digits  # Use only digits
    last_char = old_id[-1]
    available_chars = [char for char in all_chars if char != last_char]
    new_char = np.random.choice(available_chars)
    new_id = old_id[:-1] + new_char
    return new_id

# Generate new device IDs and their masked data
new_device_data = []
for device_id in device_ids:
    original_data = df_device_history[df_device_history['device_id'] == device_id]
    num_new_ids = np.random.randint(2, 5)  # Generate between 2 to 4 new device IDs
    for _ in range(num_new_ids):
        new_id = generate_new_id(device_id)
        copied_data = original_data.copy()
        copied_data['device_id'] = new_id
        masked_data = mask_data(copied_data, mask_percentage=30)
        new_device_data.append(masked_data)

# Concatenate all new device data into a single DataFrame
df_new_device_data = pd.concat(new_device_data, ignore_index=True)
print(df_new_device_data)

print(df_new_device_data['device_id'].unique())

df_new_device_data.to_csv('scenario_city_mall.csv', index=False)

