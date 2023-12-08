import json
import threading
import time
import paho.mqtt.publish as publish
import pandas as pd
from DataAnalysis import run_app
from DataCleaning import publish_clean_prices, publish_clean_station, clean_for_recent_data
from DataGathering import get_data_from_api
from DataIngesting import start_ingest_data_to_db

broker = "127.0.0.1"
latest_data = None
data_lock = threading.Lock()
last_published_prices = None
published_station_codes = set()


def fetch_data_hourly():
    global latest_data
    previous_data = None
    while True:
        new_data = get_data_from_api()
        new_data = clean_for_recent_data(new_data)
        with data_lock:
            latest_data = new_data
        if latest_data and latest_data != previous_data:
            previous_data = latest_data
        time.sleep(3600)


def publish_price_data():
    global latest_data, last_published_prices
    while True:
        new_prices_to_publish = []

        with data_lock:
            if latest_data is None:
                continue

            price_df = pd.json_normalize(latest_data['prices'])
            price_df['lastupdated'] = pd.to_datetime(price_df['lastupdated'],
                                                     format='%d/%m/%Y %H:%M:%S')  # Convert to datetime directly
            prices_list = price_df.to_dict(orient='records')

            for item in prices_list:
                item_timestamp = item['lastupdated']  # This is already a datetime object
                if last_published_prices is None or item_timestamp > last_published_prices:
                    new_prices_to_publish.append(item)

            if new_prices_to_publish:
                last_published_prices = new_prices_to_publish[-1]['lastupdated']  # Directly assign the datetime object

        for item in new_prices_to_publish:
            item['lastupdated'] = item['lastupdated'].strftime('%d/%m/%Y %H:%M:%S')
            publish.single(topic="USYD/COMP5339/yawu2780/raw_data/prices", hostname=broker, port=1883,
                           payload=json.dumps(item))


def publish_station_data():
    global latest_data, published_station_codes

    while True:
        new_stations_to_publish = []

        with data_lock:
            if latest_data is None:
                continue

            station_df = pd.json_normalize(latest_data['stations'])
            station_list = station_df.to_dict(orient='records')

            for item in station_list:
                station_code = item['code']
                if station_code not in published_station_codes:
                    new_stations_to_publish.append(item)
                    published_station_codes.add(station_code)

        for item in new_stations_to_publish:
            publish.single(topic="USYD/COMP5339/yawu2780/raw_data/stations", hostname=broker, port=1883,
                           payload=json.dumps(item))


def main():
    threading.Thread(target=fetch_data_hourly, daemon=True).start()
    print("[*] Start fetching API data every hour and stored into local database...")

    threading.Thread(target=publish_price_data, daemon=True).start()
    print("[*] Start to publish the raw price data...")

    threading.Thread(target=publish_station_data, daemon=True).start()
    print("[*] Start to publish the raw station data...")

    threading.Thread(target=publish_clean_prices, daemon=True).start()
    print("[*] Start to publish the cleaned price data...")

    threading.Thread(target=publish_clean_station, daemon=True).start()
    print("[*] Start to publish the cleaned station data...")

    threading.Thread(target=start_ingest_data_to_db, daemon=True).start()
    print("[*] Start to ingest the cleaned data...")

    run_app()


if __name__ == "__main__":
    main()
