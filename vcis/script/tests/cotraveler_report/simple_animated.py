import plotly.graph_objects as go

fig = go.Figure(
    data=[go.Scatter(x=[0], y=[0], mode='markers')],
    layout=go.Layout(
        updatemenus=[dict(
            type="buttons",
            showactive=False,
            buttons=[dict(label="Play",
                          method="animate",
                          args=[None, {"frame": {"duration": 500, "redraw": True},
                                        "fromcurrent": True,
                                        "transition": {"duration": 300,
                                                       "easing": "quadratic-in-out"}}])])]
    ),
    frames=[go.Frame(data=[go.Scatter(x=[1], y=[1], mode='markers')])]
)

fig.show()