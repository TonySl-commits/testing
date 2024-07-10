class ReportingProperties():
    # Initialize the class variables
    def __init__(self):

        # Using docstrings instead of HTML files
        self.text_box = """
            <!DOCTYPE html>
            <html lang="en">
            <head>
            <meta charset="UTF-8">
            <title>Expandable Text Box</title>
            <style>
                /* Placeholder colors for default and active states */
                .default-color {{
                    background-color: #ffffff; /* White background (default) */
                    color: #000000; /* Black text color (default) */
                    border-radius: 10px;
                    border: 1px solid #ccc; 
                }}

                .active-color {{
                    background-color: #F1F4F6; /* Different background color when active */
                    color: #000000; /* Black text color when active */
                    border: 1px solid #ccc; 
                    border-radius: 10px 10px 0px 0px; /* Rounded edges at the top */
                }}

                #description-button {{
                    cursor: pointer;
                    text-decoration: none;
                    /* border: 1px solid #ccc; */
                    /*border-radius: 10px 10px 0px 0px; /* Rounded edges at the top */
                    padding: 10px 20px;
                    display: inline-block; /* Ensures padding and border-radius are respected */
                    transition: background-color 0.3s; /* Smooth transition for background color */
                    font-family: "Bahnschrift", sans-serif;
                    font-weight: lighter;
                }}

                /* Hover style */
                #description-button:hover {{
                    background-color: #e2e6ea; /* Darker gray on hover */
                }}

                #description-content {{
                    display: none; /* Hidden initially */
                    border: 1px solid #ccc;
                    border-radius: 0px 10px 10px 10px; /* Rounded edges at the bottom */
                    padding: 10px;
                    margin-top: -1px; /* Align with the button */
                    background-color: #F1F4F6; /* Light gray background ecf3f5 for light blue*/
                    width: 98%;
                    transition: all 0.3s ease; /* Smooth expansion transition */
                    font-family: "Bahnschrift", sans-serif;
                    font-weight: lighter;
                }}

                body {{
                    margin: 0;
                    padding: 20px;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                }}
            </style>
            </head>
            <body>
            <div id="description-button" class="default-color" onclick="toggleDescription()">Click to show description</div>
            <div id="description-content">
                {content}
            </div>
            <script>
                function toggleDescription() {{
                    var button = document.getElementById('description-button');
                    var content = document.getElementById('description-content');
                    button.classList.toggle('active-color'); // Toggle the active class
                    // Apply or remove the default class
                    if (button.classList.contains('active-color')) {{
                        button.classList.remove('default-color');
                    }} else {{
                        button.classList.add('default-color');
                    }}
                    
                    // Toggle content visibility
                    if (content.style.display === 'none' || content.style.display === '') {{
                        content.style.display = 'block';
                        setTimeout(function() {{
                            content.style.opacity = 1;
                        }}, 10); // Short delay before starting opacity transition to ensure it is smooth
                    }} else {{
                        content.style.opacity = 0;
                        setTimeout(function() {{
                            content.style.display = 'none';
                        }}, 500); // Delay hiding the div until after the opacity transition finishes
                    }}
                }}
            </script>
            </body>
            </html>
            """
        
        self.aoi_key_words_description = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>AOI Key Words Description</title>
                <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css">
                <style>
                    body { font-family: "Bahnschrift", sans-serif; margin: 0; padding: 0; }
                    .container { max-width: 100%; margin: auto; padding: 20px; padding-right: 0; padding-left: 0; }
                    h1 { text-align: left; margin-bottom: 20px; font-size: 24px; padding-left: 15px;}
                    .table-striped tbody tr:nth-of-type(odd) {
                        background-color: #ecf3f5; /* Light blue */
                    }
                    .table td, .table th {
                        font-family: "Bahnschrift", sans-serif;
                        font-weight: normal;
                    }
                    /* Make the first column bold */
                    .table th:nth-child(1), .table td:nth-child(1) {
                        font-weight: bold; /* Apply bold font weight */
                        width: 25%; /* Adjust width as needed */
                    }
                    /* Make the header of the second column bold */
                    .table th:nth-child(2) {
                        font-weight: bold; /* Apply bold font weight */
                        font-family: "Bahnschrift", sans-serif; /* Ensuring font consistency */
                    }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>AOI Report</h1>
                    <table class="dataframe table table-striped table-hover">
            <thead>
                <tr style="text-align: left;">
                <th>Key Words</th>
                <th>Description</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                <td>Device Drop Down Menu</td>
                <td>This is a dropdown menu that allows the user to select a device from the list of devices.</td>
                </tr>
                <tr>
                <td>Start and End Dates</td>
                <td>Shows the start and end dates of the selected period of time.</td>
                </tr>
                <tr>
                <td>Total Hits</td>
                <td>Shows the total number of hits recorded in the selected period of time.</td>
                </tr>
                <tr>
                <td>Total Days</td>
                <td>Shows the total number of days recorded in the selected period of time.</td>
                </tr>
                <tr>
                <td>Geospatial Confidence Metrics</td>
                <td>Shows the geospatial confidence metrics for the selected device.</td>
                </tr>
                <tr>
                <td>Number of Hits Per Day of the Week</td>
                <td>Shows the number of hits per day of the week for the selected device.</td>
                </tr>
                <tr>
                <td>Number of Hits Per Month</td>
                <td>Shows the number of hits per month for the selected device.</td>
                </tr>
                <tr>
                <td>Location Likelihood with respect to Day of Week</td>
                <td>Shows the location likelihood with respect to day of week for the selected device.</td>
                </tr>
                <tr>
                <td>Estimated Current Location</td>
                <td>Shows the estimated current location for the selected device based on the device history and the current time. This also shows the expected duration of stay at the current location along with the potential next location and the latter's confidence score.</td>
                </tr>
                <tr>
                <td>Duration per Location with respect to Day of Week</td>
                <td>Shows the duration per location with respect to day of week for the selected device.</td>
                </tr>
                <tr>
                <td>Predicted Itenirary at current time</td>
                <td>Shows the path taken by the device from the latest location to the next location.</td>
                </tr>
                <tr>
                <td>Predicted Itenirary at current time AntPath</td>
                <td>Shows the path taken by the device from the latest location to the next location.</td>
                </tr>
                <tr>
                <td>Map</td>
                <td>An interactive map that shows the location of the selected device along with its coordinates, type, and number of hits.</td>
                </tr>
                <tr>
                <td>AOIs Summary Statistics</td>
                <td>Shows the different AOIs for the selected device along with their coordinates, type, number of hits, and total days recorded.</td>
                </tr>
                <tr>
                <td>Suspiciousness Evaluation</td>
                <td>Shows the suspiciousness evaluation for the selected device. This evaluation is based on how likely this device is to be in location or not based on its history.</td>
                </tr>
            </tbody>
            </table>
                </div>
            </body>
            </html>
            """

        self.base_page = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Engine Information Summary</title>
                <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css">
                <style>
                    body { font-family: "Bahnschrift", sans-serif; margin: 0; padding: 0; }
                    .container { max-width: 100%; margin: auto; padding: 20px; padding-right: 0; padding-left: 0; }
                    h1 { text-align: left; margin-bottom: 20px; font-size: 24px; padding-left: 15px;}
                    .table-striped tbody tr:nth-of-type(odd) {
                        background-color: #ecf3f5; /* Light blue */
                    }
                    .table td, .table th {
                        font-family: "Bahnschrift", sans-serif;
                        font-weight: normal;
                    }
                    /* Make the first column bold */
                    .table th:nth-child(1), .table td:nth-child(1) {
                        font-weight: bold; /* Apply bold font weight */
                        width: 25%; /* Adjust width as needed */
                    }
                    /* Make the header of the second column bold */
                    .table th:nth-child(2) {
                        font-weight: bold; /* Apply bold font weight */
                        font-family: "Bahnschrift", sans-serif; /* Ensuring font consistency */
                    }
                </style>
            </head>
            <body>
                <!-- Additional HTML content will be inserted here based on flags -->
            </body>
            </html>
            """

        self.cdr_key_words_description = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>CDR Key Words Description</title>
                <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css">
                <style>
                    body { font-family: "Bahnschrift", sans-serif; margin: 0; padding: 0; }
                    .container { max-width: 100%; margin: auto; padding: 20px; padding-right: 0; padding-left: 0; }
                    h1 { text-align: left; margin-bottom: 20px; font-size: 24px; padding-left: 15px;}
                    .table-striped tbody tr:nth-of-type(odd) {
                        background-color: #ecf3f5; /* Light blue */
                    }
                    .table td, .table th {
                        font-family: "Bahnschrift", sans-serif;
                        font-weight: normal;
                    }
                    /* Make the first column bold */
                    .table th:nth-child(1), .table td:nth-child(1) {
                        font-weight: bold; /* Apply bold font weight */
                        width: 25%; /* Adjust width as needed */
                    }
                    /* Make the header of the second column bold */
                    .table th:nth-child(2) {
                        font-weight: bold; /* Apply bold font weight */
                        font-family: "Bahnschrift", sans-serif; /* Ensuring font consistency */
                    }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>CDR Report</h1>
                    <!-- Table will be filled later by the user -->
                </div>
            </body>
            </html>
            """

        self.consistency_metrics_description = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Consistency Metrics Description</title>
                <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css">
                <style>
                    body { font-family: "Bahnschrift", sans-serif; margin: 0; padding: 0; }
                    .container { max-width: 100%; margin: auto; padding: 20px; padding-right: 0; padding-left: 0; }
                    h1 { text-align: left; margin-bottom: 20px; font-size: 24px; padding-left: 15px;}
                    .table-striped tbody tr:nth-of-type(odd) {
                        background-color: #ecf3f5; /* Light blue */
                    }
                    .table td, .table th {
                        font-family: "Bahnschrift", sans-serif;
                        font-weight: normal;
                    }
                    /* Make the first column bold */
                    .table th:nth-child(1), .table td:nth-child(1) {
                        font-weight: bold; /* Apply bold font weight */
                        width: 25%; /* Adjust width as needed */
                    }
                    /* Make the header of the second column bold */
                    .table th:nth-child(2) {
                        font-weight: bold; /* Apply bold font weight */
                        font-family: "Bahnschrift", sans-serif; /* Ensuring font consistency */
                    }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>Consistency Metrics</h1>
                    <table class="dataframe table table-striped table-hover">
            <thead>
                <tr style="text-align: left;">
                <th>Metrics</th>
                <th>Description</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                <td>Time Period Consistency</td>
                <td>This metric evaluates the consistency of days recorded over the specified period of time.</td>
                </tr>
                <tr>
                <td>Hour of Day Consistency</td>
                <td>This metric assesses the consistency of data recorded throughout the day.</td>
                </tr>
                <tr>
                <td>Day of Week Consistency</td>
                <td>This metric assesses the consistency of recorded data with respect to the Day of Week.</td>
                </tr>
                <tr>
                <td>Hits Per Day Consistency</td>
                <td>This metric evaluates the consistency of activity levels recorded per day, providing insights into user behavior patterns throughout a day.</td>
                </tr>
                <tr>
                <td>Confidence Score</td>
                <td>This score provides an overall measure of confidence in the analysis results, based on the availability and consistency of geospatial data. This metrics, based on the aforementioned consistency metrics, ranges between 0 and 100, and indicates that the data and analysis results are reliable the higher this score is.</td>
                </tr>
            </tbody>
            </table>
                </div>
            </body>
            </html>
            """

        self.cotraveler_key_words_description = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Cotraveler Key Words Description</title>
                <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css">
                <style>
                    body { font-family: "Bahnschrift", sans-serif; margin: 0; padding: 0; }
                    .container { max-width: 100%; margin: auto; padding: 20px; padding-right: 0; padding-left: 0; }
                    h1 { text-align: left; margin-bottom: 20px; font-size: 24px; padding-left: 15px;}
                    .table-striped tbody tr:nth-of-type(odd) {
                        background-color: #ecf3f5; /* Light blue */
                    }
                    .table td, .table th {
                        font-family: "Bahnschrift", sans-serif;
                        font-weight: normal;
                    }
                    /* Make the first column bold */
                    .table th:nth-child(1), .table td:nth-child(1) {
                        font-weight: bold; /* Apply bold font weight */
                        width: 25%; /* Adjust width as needed */
                    }
                    /* Make the header of the second column bold */
                    .table th:nth-child(2) {
                        font-weight: bold; /* Apply bold font weight */
                        font-family: "Bahnschrift", sans-serif; /* Ensuring font consistency */
                    }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>Cotraveler Report</h1>
                    <!-- Table will be filled later by the user -->
                </div>
            </body>
            </html>
            """
