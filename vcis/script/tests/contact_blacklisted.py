import pandas as pd

# Sample data setup
data1 = {
    'group_id': [0],
    'Device ID': [['79bbd379-5c9e-4d7c-ac7f-84119eec9a3f', '95cca195-2b5e-4a9b-ab9f-48995eeb5d1f', '8AC9DACF-BCE4-4247-A033-185A7E71E820']]
}

data2 = {
    'Device ID': ['8AC9DACF-BCE4-4247-A033-185A7E71E820', '95cca195-2b5e-4a9b-ab9f-48995eeb5d1f', '79bbd379-5c9e-4d7c-ac7f-84119eec9a3f'],
    'Contaced Black Listed Device(s)': [[], [], ['7']]
}

# Convert the list of Device IDs in data1 to a DataFrame and explode it
df1 = pd.DataFrame(data1)
df1_exploded = df1.explode('Device ID')

# Convert data2 to a DataFrame and explode the 'Contaced Black Listed Device(s)'
df2 = pd.DataFrame(data2)
df2_exploded = df2.explode('Contaced Black Listed Device(s)')


print("df1",df1_exploded)
print("df2",df2_exploded)

# Merge df1 and df2_exploded on 'Device ID'
merged_df = df1_exploded.merge(df2_exploded, on='Device ID', how='left')
print(merged_df)
# Check if any device in each group has contacted a blacklisted device
group_contact_blacklist = merged_df.groupby('group_id')['Contaced Black Listed Device(s)'].apply(lambda x: x.notna().any()).reset_index()
print(group_contact_blacklist)
# Rename the column for clarity
group_contact_blacklist.rename(columns={'Contaced Black Listed Device(s)': 'Contacted_Blacklisted'}, inplace=True)
print(group_contact_blacklist)
# Replace True/False with 'Yes'/'No'
group_contact_blacklist['Contacted_Blacklisted'] = group_contact_blacklist['Contacted_Blacklisted'].map({True: 'Yes', False: 'No'})

print(group_contact_blacklist)