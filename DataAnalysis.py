from copy import deepcopy
import dash
import pandas as pd
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import paho.mqtt.subscribe as subscribe
import json
from datetime import datetime
import numpy as np
import threading
import plotly.express as px
from dash.exceptions import PreventUpdate

broker = "127.0.0.1"
price_list = []
station_list = []


# Function to update the data structure based on received message
def process_price_msg(client, userdata, msg):
    global price_list
    data = json.loads(msg.payload)
    price_list.append(data)


def process_station_msg(client, userdata, msg):
    global station_list
    data = json.loads(msg.payload)
    station_list.append(data)


# MQTT subscription in a separate thread
def mqtt_price_subscribe():
    print("[*] Connected to USYD/COMP5339/yawu2780/cleaned_data/prices")
    subscribe.callback(process_price_msg, topics="USYD/COMP5339/yawu2780/cleaned_data/prices", hostname=broker,
                       port=1883)


def mqtt_station_subscribe():
    print("[*] Connected to USYD/COMP5339/yawu2780/cleaned_data/station")
    subscribe.callback(process_station_msg, topics="USYD/COMP5339/yawu2780/cleaned_data/stations", hostname=broker,
                       port=1883)


app = dash.Dash(__name__)

# Define the app layout
app.layout = html.Div([
    html.H3("COMP5339 Assignment: Real-time Fuel Prices Monitoring Dash Board Across NSW",
            style={'text-align': 'center'}),
    html.H4("Unikey: yawu2780", style={'text-align': 'right'}),
    html.Hr(),
    html.Div([
        dcc.Graph(id='live-bar-graph', style={'display': 'inline-block', 'width': '49%'}),
        dcc.Graph(id='live-map-graph', style={'display': 'inline-block', 'width': '49%'}),
    ], style={'width': '100%', 'display': 'flex'}),
    dcc.Graph(id='live-line-graph'),
    dcc.Interval(
        id='interval-component',
        interval=1 * 1000,  # in milliseconds
        n_intervals=0
    )
])


@app.callback(Output('live-bar-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_bar_graph(n):
    fuel_data = {}
    for data in price_list:
        fueltype = data["fueltype"]
        price = data["price"]
        if fueltype in fuel_data:
            fuel_data[fueltype].append(price)
        else:
            fuel_data[fueltype] = [price]

    traces = []
    for fueltype, prices in fuel_data.items():
        avg_price = round(float(np.mean(prices)), 2)  # Calculating the average price directly
        traces.append(go.Bar(x=[fueltype], y=[avg_price], text=[avg_price], textposition='outside', name=fueltype))

    fig = go.Figure(traces)
    fig.update_layout(title="Bar chart of average prices for different fuel types")
    fig.update_layout(margin={"r": 50, "t": 50, "l": 50, "b": 50})

    return fig


@app.callback(Output('live-map-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_map_graph(n):
    if not station_list or not price_list:
        raise PreventUpdate

    # Deep copy the lists to ensure consistency
    stations_data = deepcopy(station_list)
    prices_data = deepcopy(price_list)

    # Create dataframes from the deep copied lists
    stations_df = pd.DataFrame(stations_data)
    price_df = pd.DataFrame(prices_data)

    if stations_df.empty or price_df.empty:
        raise PreventUpdate

    price_df['lastupdated'] = pd.to_datetime(price_df['lastupdated'], format='%d/%m/%Y %H:%M:%S')
    latest_prices = price_df.sort_values('lastupdated').groupby(['stationcode', 'fueltype']).last().reset_index()

    # Convert columns to int64 if needed
    if stations_df['code'].dtype == 'object':
        stations_df['code'] = stations_df['code'].astype('int64')
    if latest_prices['stationcode'].dtype == 'object':
        latest_prices['stationcode'] = latest_prices['stationcode'].astype('int64')

    merged_df = pd.merge(stations_df, latest_prices, left_on='code', right_on='stationcode', how='left')
    merged_df['fuelinfo'] = merged_df.apply(
        lambda row: f"{row['fueltype']}: {row['price']}" if pd.notnull(row['price']) else "", axis=1
    )

    hover_data = merged_df.groupby(['name', 'brand', 'address', 'location.latitude', 'location.longitude'])[
        'fuelinfo'].apply(lambda x: "<br>".join(x)).reset_index()

    fig = px.scatter_mapbox(hover_data,
                            lat='location.latitude',
                            lon='location.longitude',
                            hover_name='name',
                            hover_data={'name': False, 'brand': True, 'address': True, 'fuelinfo': True},
                            color_discrete_sequence=["red"],
                            zoom=5.5,
                            height=500)

    fig.update_layout(mapbox_style="carto-positron")
    fig.update_layout(margin={"r": 50, "t": 50, "l": 50, "b": 50})
    fig.update_layout(title="Maps of service stations")

    return fig


# Define callback to update line chart
@app.callback(Output('live-line-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_line_graph(n):
    fuel_data = {}
    for data in price_list:
        fueltype = data["fueltype"]
        price = data["price"]
        timestamp = datetime.strptime(data["lastupdated"], "%d/%m/%Y %H:%M:%S")
        if fueltype in fuel_data:
            fuel_data[fueltype].append((timestamp, price))
        else:
            fuel_data[fueltype] = [(timestamp, price)]

    traces = []
    for fueltype, values in fuel_data.items():
        values.sort(key=lambda x: x[0])  # Sorting by timestamp
        times, prices = zip(*values)
        traces.append(go.Scatter(x=times, y=prices, mode='lines', name=fueltype))  # Using raw prices directly

    fig = go.Figure(traces)
    fig.update_layout(title="Graphs of time trends in prices for different fuel types")
    fig.update_layout(margin={"r": 50, "t": 50, "l": 50, "b": 50})

    return fig


def run_app():
    # Start the MQTT subscription thread
    threading.Thread(target=mqtt_price_subscribe, daemon=True).start()
    print("[*] Start to receive the cleaned price data...")

    threading.Thread(target=mqtt_station_subscribe, daemon=True).start()
    print("[*] Start to receive the cleaned price data...")

    # Create a Dash app
    app.run_server(debug=False)
