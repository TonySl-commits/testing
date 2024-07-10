import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

# Define the colors for the gradient; dark blue to light blue
colors = ["#03045E", "#90E0EF"]

# Create a new colormap from the listed colors
cmap = LinearSegmentedColormap.from_list("custom_blue", colors, N=256)

# Create some data to display
data = np.random.randn(10, 10)

# Displaying the data with the colormap
plt.imshow(data, interpolation='nearest', cmap=cmap)
plt.colorbar()
plt.show()
