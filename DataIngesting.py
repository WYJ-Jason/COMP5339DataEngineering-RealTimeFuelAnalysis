import json
import sqlite3
import threading
import pandas as pd
from paho.mqtt import subscribe

broker = "127.0.0.1"
price_list = []
station_list = []


def create_tables():
    conn = sqlite3.connect('FuelAnalysis.db')

    # Delete the tables if they exist
    conn.execute('DROP TABLE IF EXISTS stations')
    conn.execute('DROP TABLE IF EXISTS prices')

    # Creating stations table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS stations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            brandid VARCHAR(255),
            stationid VARCHAR(255),
            brand VARCHAR(255),
            code VARCHAR(255),
            name VARCHAR(255),
            address VARCHAR(255),
            location_latitude FLOAT,
            location_longitude FLOAT
        )
    ''')

    # Creating prices table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stationcode VARCHAR(255),
            fueltype VARCHAR(255),
            price FLOAT,
            lastupdated TIMESTAMP
        )
    ''')

    conn.close()


def save_to_sql(df, database_name, table_name):
    conn = sqlite3.connect(database_name)
    df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.commit()
    conn.close()


def store_in_db(data):
    # Determine whether the data is a station or a price based on the keys present
    if 'code' in data:
        data['location_latitude'] = data.pop('location.latitude')
        data['location_longitude'] = data.pop('location.longitude')
        station_df = pd.DataFrame([data])
        save_to_sql(station_df, 'FuelAnalysis.db', 'stations')
    elif 'stationcode' in data:
        price_df = pd.DataFrame([data])
        save_to_sql(price_df, 'FuelAnalysis.db', 'prices')


def process_price_msg_to_db(client, userdata, message):
    data = json.loads(message.payload)
    store_in_db(data)


def process_station_msg_to_db(client, userdata, message):
    data = json.loads(message.payload)
    store_in_db(data)


def mqtt_price_subscribe():
    print("[*] Connected to USYD/COMP5339/yawu2780/cleaned_data/prices")
    subscribe.callback(process_price_msg_to_db, topics="USYD/COMP5339/yawu2780/cleaned_data/prices", hostname=broker,
                       port=1883)


def mqtt_station_subscribe():
    print("[*] Connected to USYD/COMP5339/yawu2780/cleaned_data/station")
    subscribe.callback(process_station_msg_to_db, topics="USYD/COMP5339/yawu2780/cleaned_data/stations",
                       hostname=broker, port=1883)


def start_ingest_data_to_db():
    create_tables()
    threading.Thread(target=mqtt_price_subscribe, daemon=True).start()
    threading.Thread(target=mqtt_station_subscribe, daemon=True).start()
