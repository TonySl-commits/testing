import pandas as pd
from vcis.utils.utils import CDR_Properties,CDR_Utils
utils= CDR_Utils()

properties = CDR_Properties()   
df = pd.read_csv(properties.passed_filepath_excel+'df_history_backup.csv')
# 2A0CA0C9-DD9A-4BDA-A914-5B80FA7AD77E
# 22a9f8a3-127e-49e3-a719-79431e48fd81

df = df[(df['device_id'] == '2A0CA0C9-DD9A-4BDA-A914-5B80FA7AD77E') | (df['device_id'] == '22a9f8a3-127e-49e3-a719-79431e48fd81')]

print(df)
df['usage_timeframe'] = pd.to_datetime(df['usage_timeframe'])

from datetime import datetime, timedelta
from stonesoup.types.detection import Detection
from stonesoup.types.groundtruth import GroundTruthPath, GroundTruthState
from stonesoup.types.track import Track
from stonesoup.types.array import StateVector
from stonesoup.plotter import AnimationPlotter
import matplotlib

# Assuming df is your DataFrame with columns: ['device_id', 'usage_timeframe', 'latitude', 'longitude']
# Convert DataFrame to Stone Soup Tracks
groundtruth_paths = {}
for device_id, group in df.groupby('device_id'):
    states = [GroundTruthState(StateVector([lat, lon]), timestamp=time)
               for lat, lon, time in zip(group['location_latitude'], group['location_longitude'], group['usage_timeframe'])]
    groundtruth_paths[device_id] = GroundTruthPath(states)

# Assuming you want to plot all detections as TrueDetections
detections = [Detection(StateVector([lat, lon]), timestamp=time, metadata={'device_id': device_id})
             for device_id, group in df.groupby('device_id')
             for lat, lon, time in zip(group['location_latitude'], group['location_longitude'], group['usage_timeframe'])]

# Assuming you want to plot all tracks as a single track (you might need to adjust this based on your requirements)
tracks = [Track(states=[Detection(StateVector([lat, lon]), timestamp=time, metadata={'device_id': device_id})
                 for lat, lon, time in zip(group['location_latitude'], group['location_longitude'], group['usage_timeframe'])])
           for device_id, group in df.groupby('device_id')]

# Plotting
plotter = AnimationPlotter(legend_kwargs=dict(loc='upper left'))

plotter.plot_ground_truths(groundtruth_paths.values(), mapping=[0, 1])
plotter.plot_measurements(detections, mapping=[0, 1])
plotter.plot_tracks(tracks, mapping=[0, 1])

matplotlib.rcParams['animation.html'] = 'jshtml'
plotter.run(plot_item_expiry=timedelta(seconds=60))
plotter.save('example_animation.gif')