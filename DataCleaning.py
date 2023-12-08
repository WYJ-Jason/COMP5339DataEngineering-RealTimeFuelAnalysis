import datetime
import threading
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import json
from datetime import datetime, timedelta
import pandas as pd

broker = "127.0.0.1"
clean_price = []
clean_station = []
data_lock = threading.Lock()  # Lock for synchronizing access to clean_price


def clean_for_recent_data(data):
    # Normalize data
    price_df = pd.json_normalize(data['prices'])

    # Convert 'lastupdated' column to datetime format
    price_df['lastupdated'] = pd.to_datetime(price_df['lastupdated'], format='%d/%m/%Y %H:%M:%S')

    # Determine the latest date in 'prices'
    latest_date = price_df['lastupdated'].max()

    print(f"Latest price record release date and time: {latest_date}")

    # Calculate the starting date for the last month based on the latest date
    one_month_before_latest = latest_date - timedelta(days=30)

    # Filter for prices from the last month based on the latest date
    recent_price_df = price_df[price_df['lastupdated'] > one_month_before_latest]

    # Sort by 'lastupdated' in descending order
    sorted_recent_price_df = recent_price_df.sort_values(by='lastupdated', ascending=True)

    # Convert back to original data format
    data['prices'] = sorted_recent_price_df.to_dict(orient='records')

    return data


def clean_price_data(record):
    # 1. Consistency: Ensure that keys exist in the record.
    expected_keys = ['stationcode', 'fueltype', 'price', 'lastupdated']
    for key in expected_keys:
        if key not in record:
            raise ValueError(f"Key {key} missing from record")

    # 2. Missing Values: Check for null values.
    for key, value in record.items():
        if value is None or value == "":
            raise ValueError(f"Missing or null value for {key}")

    # Remove records with price as 0
    if record['price'] == 0:
        return None

    # 3. Data Types: Check and correct data types.
    if not isinstance(record['stationcode'], str):
        record['stationcode'] = str(record['stationcode'])

    if not isinstance(record['fueltype'], str):
        raise TypeError(f"Invalid type for fueltype: {type(record['fueltype'])}")

    if not isinstance(record['price'], (int, float)):
        try:
            record['price'] = float(record['price'])
        except ValueError:
            raise TypeError(f"Invalid type for price: {type(record['price'])}")

    # Convert 'lastupdated' to a datetime object for uniformity
    try:
        date_format = "%d/%m/%Y %H:%M:%S"
        record['lastupdated'] = datetime.strptime(record['lastupdated'], date_format)
    except ValueError:
        raise TypeError(f"Invalid date format for lastupdated: {record['lastupdated']}")

    # Convert the datetime object back to string in the desired format for JSON serialization
    record['lastupdated'] = record['lastupdated'].strftime(date_format)

    return record


def clean_station_data(record):
    # 1. Consistency: Ensure that keys exist in the record.
    expected_keys = ['brandid', 'stationid', 'brand', 'code', 'name', 'address', 'location.latitude',
                     'location.longitude']
    for key in expected_keys:
        if key not in record:
            raise ValueError(f"Key {key} missing from record")

    # 2. Missing Values: Check for and handle any missing or null values.
    for key, value in record.items():
        if value is None:
            raise ValueError(f"Missing value for {key}")
        if value == "":
            if key == "brandid":
                record['brandid'] = record['brand']
            elif key == "stationid":
                record['stationid'] = record['code']

    # 3. Data Types: Check and correct data types.
    if not isinstance(record['brandid'], str):
        record['brandid'] = str(record['brandid'])

    if not isinstance(record['stationid'], str):
        record['stationid'] = str(record['stationid'])

    if not isinstance(record['brand'], str):
        raise TypeError(f"Invalid type for brand: {type(record['brand'])}")

    if not isinstance(record['code'], str):
        record['code'] = str(record['code'])

    if not isinstance(record['name'], str):
        raise TypeError(f"Invalid type for name: {type(record['name'])}")

    if not isinstance(record['address'], str):
        raise TypeError(f"Invalid type for address: {type(record['address'])}")

    if not isinstance(record['location.latitude'], (int, float)):
        try:
            record['location.latitude'] = float(record['location.latitude'])
        except ValueError:
            raise TypeError(f"Invalid type for location.latitude: {type(record['location.latitude'])}")

    if not isinstance(record['location.longitude'], (int, float)):
        try:
            record['location.longitude'] = float(record['location.longitude'])
        except ValueError:
            raise TypeError(f"Invalid type for location.longitude: {type(record['location.longitude'])}")

    return record


def on_price_connect(client, userdata, flags, rc):
    print("[*] Connected to USYD/COMP5339/yawu2780/raw_data/prices")
    client.subscribe("USYD/COMP5339/yawu2780/raw_data/prices")


def on_station_connect(client, userdata, flags, rc):
    print("[*] Connected to USYD/COMP5339/yawu2780/raw_data/stations")
    client.subscribe("USYD/COMP5339/yawu2780/raw_data/stations")


def on_price_message(client, userdata, msg):
    global clean_price
    data = json.loads(msg.payload)
    try:
        with data_lock:  # Ensure thread-safety when accessing clean_price
            clean_data = clean_price_data(data)
            clean_price.append(clean_data)
            # print(f'Cleaned price data: {clean_data}')
    except ValueError as e:
        print(f"Removed invalid data: {data}, reason: {str(e)}")


def on_station_message(client, userdata, msg):
    global clean_station
    data = json.loads(msg.payload)
    try:
        with data_lock:
            clean_data = clean_station_data(data)
            clean_station.append(clean_data)
            # print(f'Cleaned station data: {clean_data}')
    except ValueError as e:
        print(f"Removed invalid data: {data}, reason: {str(e)}")


def raw_prices_subscribe():
    client = mqtt.Client()
    client.on_connect = on_price_connect
    client.on_message = on_price_message
    client.connect(broker, 1883, 60)
    client.loop_start()  # Starts a new thread that calls loop_forever()


def raw_station_subscribe():
    client = mqtt.Client()
    client.on_connect = on_station_connect
    client.on_message = on_station_message
    client.connect(broker, 1883, 60)
    client.loop_start()  # Starts a new thread that calls loop_forever()


def publish_clean_prices():
    threading.Thread(target=raw_prices_subscribe, daemon=True).start()

    global clean_price
    while True:
        with data_lock:
            if clean_price:
                for item in clean_price:
                    publish.single(topic="USYD/COMP5339/yawu2780/cleaned_data/prices", payload=json.dumps(item),
                                   hostname=broker, port=1883)
                clean_price = []


def publish_clean_station():
    threading.Thread(target=raw_station_subscribe, daemon=True).start()

    global clean_station
    while True:
        with data_lock:
            if clean_station:
                for item in clean_station:
                    publish.single(topic="USYD/COMP5339/yawu2780/cleaned_data/stations", payload=json.dumps(item),
                                   hostname=broker, port=1883)
                clean_station = []
