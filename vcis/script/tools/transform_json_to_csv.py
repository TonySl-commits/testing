import pandas as pd
import ast  # Import ast which will help to safely evaluate string literals as Python expressions

# Read the text file data
with open('C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/jsonnn.txt', 'r') as file:
    data = file.read()

# Convert the string data to a Python list
data_list = ast.literal_eval(data)

# Create a DataFrame
df = pd.DataFrame(data_list, columns=['Latitude', 'Longitude', 'device_id', 'Timestamp', 'Another_device_id', 'Provider'])

df.to_csv('C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/data_zaher.csv')
# Display the DataFrame to check it
print(df.head())