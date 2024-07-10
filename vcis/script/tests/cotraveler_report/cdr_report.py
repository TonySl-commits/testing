import pandas as pd
import plotly.express as px
from vcis.utils.utils import CDR_Utils

utils = CDR_Utils(verbose=True)

cdr_data = pd.read_excel('/u01/jupyter-scripts/Joseph/CDR_trace/data/cdr/results/trace_152291897696890_1.xlsx')
cdr_data = utils.convert_ms_to_datetime(cdr_data)

cdr_data['usage_timeframe'] = pd.to_datetime(cdr_data['usage_timeframe'])
cdr_data['day'] = cdr_data['usage_timeframe'].dt.date
cdr_data['location'] = 'LEB'

cdr_data['visit_count'] = cdr_data.groupby('cgi_id')['cgi_id'].transform('count')



print(cdr_data.head())
import plotly.graph_objects as go
import numpy as np
cdr_data['text'] = '<br>Cell Tower Name ' + cdr_data['cgi_id'] 
# limits = [(0,2),(3,10),(11,20),(21,50),(50,3000)]
colors = ["royalblue","crimson","lightseagreen","orange","lightgrey"]
cities = []
scale = 500
limits = cdr_data['cgi_id'].unique().tolist()

fig = go.Figure()

for i in range(len(limits)):
    lim = limits[i]
    df_sub = cdr_data[cdr_data ['cgi_id'] == limits[i]]
    print(df_sub['visit_count'].sum()/scale)
    fig.add_trace(go.Scattergeo(
        locationmode = 'country names',
        locations = ['Lebanon'],
        lon = df_sub['location_latitude'],
        lat = df_sub['location_longitude'],
        text = df_sub['text'],
        marker = dict(
            size = df_sub['visit_count'].sum()/scale,
            line_color='rgb(40,40,40)',
            line_width=0.5,
            sizemode = 'area'
        ),
        name = ''.format(lim)))

fig.update_layout(
        title_text = '2014 US city populations<br>(Click legend to toggle traces)',
        showlegend = True,
        geo = dict(
            scope = 'asia',
            landcolor = 'rgb(217, 217, 217)',
        )
    )

fig.show()
