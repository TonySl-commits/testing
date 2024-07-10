import plotly.express as px
import pandas as pd
import plotly.graph_objects as go
from vcis.utils.utils import CDR_Utils, CDR_Properties
import calendar
properties= CDR_Properties()

data = pd.read_csv(properties.passed_filepath_excel+'df_history_backup.csv')
data['day'] = pd.to_datetime(data['usage_timeframe']).dt.day_name()

# Count hits per day of the week
hits_per_day = data['day'].value_counts().reset_index()
hits_per_day.columns = ['day', 'nb_hits']

# Ensure all days of the week are present, even if no hits
order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
all_days = pd.DataFrame({'day': order})
hits_per_day = pd.merge(all_days, hits_per_day, how='left', on='day').fillna(0)

# Create bar plot with text labels inside the bars
fig = px.bar(hits_per_day, x='day', y='nb_hits', title='Number of Hits per Day of the Week',
            labels={'day': 'Day of Week', 'nb_hits': 'Number of Hits'},
            text='nb_hits')  # Use 'NumberOfHits' column for text labels

# Layout for title and axes font
fig.update_layout(
    title={
        'text': 'Number of Hits per Day of the Week',
        'font': dict(family="Bahnschrift SemiBold", size=20, color='#03045E')
    },
    xaxis_title_text='Day of Week',  # X-axis title
    yaxis_title_text='Number of Hits',  # Y-axis title
    xaxis=dict(
        titlefont= dict(family="Bahnschrift SemiBold", size=15, color='#03045E'),
        tickfont= dict(family="Bahnschrift SemiBold", size=15, color='#03045E')
    ),
    yaxis=dict(
        titlefont= dict(family="Bahnschrift SemiBold", size=15, color='#03045E'),
        tickfont= dict(family="Bahnschrift SemiBold", size=15, color='#03045E')
    )
)

fig.update_layout({'paper_bgcolor': 'rgba(255, 255, 255, 1)',
                                'plot_bgcolor': 'rgba(255, 255, 255, 1)'})

fig.update_traces(marker_color=['#03045E', '#023E8A', '#0077B6', '#0096C7', '#00B4D8', 
                                '#48CAE4', '#90E0EF', '#ADE8F4', '#CAF0F8'], textposition='inside', textfont=dict(color='white'))  # Set text position inside the bars and white color for text

fig.write_html('number_of_hits_dow.html')

##############################################################################################################################
data['usage_timeframe'] = pd.to_datetime(data['usage_timeframe'])

# Generate the year and month string from 'Timestamp'
data['month'] = data['usage_timeframe'].dt.to_period('M')

# Aggregate data by Month and count the number of hits
hits_per_month = data.groupby('month').size().reset_index(name='NumberOfHits')

# Create a DataFrame for all months in the year
year = data['usage_timeframe'].dt.year.iloc[0]  # Use the year from the first record
all_months = [f"{year}-{month:02d}" for month in range(1, 13)]
all_months_df = pd.DataFrame({'month': pd.PeriodIndex(all_months, freq='M')})

# Merge the aggregated data with the all_months_df DataFrame
merged_data = pd.merge(all_months_df, hits_per_month, on='month', how='left').fillna(0)

# Convert 'Month' to timestamps to extract the month names
merged_data['month'] = merged_data['month'].dt.start_time
merged_data['monthName'] = merged_data['month'].dt.month.apply(lambda x: calendar.month_name[x])

# Create bar plot with text labels inside the bars
fig = px.bar(merged_data, x='monthName', y='NumberOfHits', title='Number of Hits per Month',
            color='monthName', color_discrete_sequence=['#1130A7', '#125F92', '#128F7E', '#13BE69', '#13EE55', '#A4D82F', 
                                    '#ECCD1C', '#FC9C14', '#CB620F', '#9A270A', '#561463', '#1100BB'],
            text='NumberOfHits')  # Use 'NumberOfHits' column for text labels

# Layout for title and axes font
fig.update_layout(
    title={
        'text': 'Number of Hits per Month',
        'font': dict(family="Bahnschrift SemiBold", size=20, color='#03045E')
    },
    xaxis_title_text='Months',  # X-axis title
    yaxis_title_text='Number of Hits',  # Y-axis title
    xaxis=dict(
        titlefont=dict(family="Bahnschrift SemiBold", size=15, color='#03045E'),
        tickfont=dict(family="Bahnschrift SemiBold", size=15, color='#03045E')
    ),
    yaxis=dict(
        titlefont=dict(family="Bahnschrift SemiBold", size=15, color='#03045E'),
        tickfont=dict(family="Bahnschrift SemiBold", size=15, color='#03045E')
    )
)

fig.update_layout({'paper_bgcolor': 'rgba(255, 255, 255, 1)',
                                'plot_bgcolor': 'rgba(255, 255, 255, 1)'})

fig.update_traces(textposition='inside', textfont=dict(color='white'))  # Set text position inside the bars and white color for text

fig.write_html('number_of_hits_dom.html')
