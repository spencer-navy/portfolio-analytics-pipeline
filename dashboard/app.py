# Import necessary libraries
from dash import Dash, html, dcc, Input, Output
from data.redis_client import RedisDataFetcher
import pandas as pd
import plotly.graph_objects as go

# incorporate data
fetcher = RedisDataFetcher()
data = fetcher.get_all_dashboard_data()
timeline = data['events_timeline']

app = Dash()

app.layout = [
    html.Div(children='Hello, Dash!'),
    html.Pre(str(data)),
    
    dcc.Graph(id='my-graph'),  # Add the graph component
    dcc.Interval(id='interval', interval=5000, n_intervals=0)
]

@app.callback(
    Output('my-graph', 'figure'),
    Input('interval', 'n_intervals')
)
def update_graph(n):
    data = fetcher.get_all_dashboard_data()
    timeline = data['events_timeline']
    
    # Convert to dataframe
    df = pd.DataFrame(timeline)
    
    # Create figure
    figure = go.Figure()
    figure.add_trace(go.Scatter(
        x=df['datetime'],
        y=df['count'],
        mode='lines+markers'
    ))
    figure.update_layout(
        title='Events Over Time',
        xaxis_title='Time',
        yaxis_title='Event Count'
    )
    
    return figure

if __name__ == '__main__':
    app.run(debug=True)