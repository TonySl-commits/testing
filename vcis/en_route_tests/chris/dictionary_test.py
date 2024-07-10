import pandas as pd

# Example data
data = {
    'device1': {
        'sensor1': pd.DataFrame({'A': [1, 2], 'B': [3, 4]}),
        'sensor2': pd.DataFrame({'A': [5, 6, 7], 'B': [8, 9, 10]})
    },
    'device2': {
        'sensor1': pd.DataFrame({'A': [11, 12], 'B': [13, 14]}),
        'sensor2': pd.DataFrame({'A': [15], 'B': [16]})
    },
    'device3': {
        'sensor1': pd.DataFrame({'A': [17, 18, 19], 'B': [20, 21, 22]}),
        'sensor2': pd.DataFrame({'A': [23, 24, 25, 26], 'B': [27, 28, 29, 30]})
    }
}

# Calculate the total DataFrame lengths for each device
device_lengths = {device_id: sum(df.shape[0] for df in sensors.values()) for device_id, sensors in data.items()}

# Sort the devices by the total DataFrame lengths
sorted_device_ids = sorted(device_lengths, key=device_lengths.get)

# Optionally, create a new dictionary sorted by these lengths
sorted_data = {device_id: data[device_id] for device_id in sorted_device_ids}

print(sorted_device_ids)
print(sorted_data)
