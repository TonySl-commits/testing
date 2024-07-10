from vcis.correlation.cdr_correlation_functions import CDRCorrelationFunctions

correlation_functions = CDRCorrelationFunctions()

start_date = '2023-01-01'
end_date = '2023-12-30'

# latitude, longitude = 33.89762553662286, 35.56575722899548

# latitude, longitude = 33.90032072485701, 35.4749686236271

# latitude, longitude = 33.900068110991505, 35.50553657603517

# latitude, longitude = 33.88253709559792, 35.51128065763815

# latitude, longitude = 33.92982568595689, 35.58750599183707

latitude, longitude = 33.88843867964114, 35.519771828581355

scan_distance = 500

start_date = correlation_functions.str_date_to_millisecond(start_date)
end_date = correlation_functions.str_date_to_millisecond(end_date)

devices, device_list = correlation_functions.get_device_list_from_device_scan(start_date = start_date,
                                                                     end_date = end_date, 
                                                                     latitude = latitude,
                                                                     longitude = longitude,
                                                                     scan_distance = scan_distance)

print(len(device_list))

# Get the important devices only
devices = devices.groupby(['device_id']).size().reset_index(name='Total Hits')
devices.sort_values(by='Total Hits', ascending=False, inplace=True)

important_devices = devices[devices['Total Hits'] >= 1000]['device_id'].unique().tolist()
print('important_devices', important_devices)
print(len(important_devices))

