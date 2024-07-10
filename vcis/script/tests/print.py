import time
import sys

def animate_loading():
    chars = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    for char in chars:
        sys.stdout.write('\r' + 'Loading ' + char)
        time.sleep(0.1)
        sys.stdout.flush()

# Example usage
animate_loading()