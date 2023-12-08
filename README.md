
# COMP5339 Assignment: Real-time Fuel Prices Monitoring Dash Board Across NSW

## Introduction
This project is an interactive data visualization tool that provides real-time monitoring of fuel prices and station distribution in New South Wales. It is designed to work with a local MQTT server, ensuring that data remains within the network and is easily accessible.

## Table of Contents
- [Requirements](#requirements)
- [Installation](#installation)
  - [Local MQTT Server](#local-mqtt-server)
  - [Dependency Installation](#dependency-installation)
- [Usage](#usage)
- [Contact](#contact)

## Requirements
Before running the dashboard, ensure that you have the following:
- A local MQTT server
- Python 3.9 or later
- All the dependencies listed in `requirements.txt`

## Installation

### Local MQTT Server
To install a local MQTT server, please follow the instructions at [Mosquitto.org](https://mosquitto.org/download/). Ensure the server is running on `127.0.0.1`.

### Dependency Installation
Install all required dependencies by running the following command:
```
pip install -r requirements.txt
```

## Usage
To start the dashboard, execute:
```
python main.py
```
Upon running `main.py`, the dashboard address and logs will appear in the terminal.


## Contact
For help and support, please contact yawu2780@uni.sydney.edu.au.
