import requests
from datetime import datetime


def get_data_from_api():
    token_url = 'https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken'

    grant_type = {'grant_type': 'client_credentials'}

    headers = {
        'Authorization': "Basic RDhtRkFQSENnUWNpbFl2UEtNa0JlUDQySHhHSnFaMHQ6Y3psaVBKMnNqSGRKclNXOA==",
        'Accept': 'application/json'
    }

    response = requests.get(token_url, headers=headers, params=grant_type)

    try:
        token = response.json()['access_token']
        authorization = 'Bearer ' + token
        prices_url = "https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices"

        headers = {
            'Authorization': authorization,
            'Accept': "application/json",
            'Content-type': "application/json",
            'apikey': "D8mFAPHCgQcilYvPKMkBeP42HxGJqZ0t",
            'transactionid': "0030",
            'requesttimestamp': datetime.now().strftime("%d/%m/%Y %I:%M:%S %p"),
        }

        try:
            data_response = requests.get(prices_url, headers=headers)
            data = data_response.json()
            return data
        except Exception as e:
            print(f"Error: {e}\nFailed to get data from API. Status code: {response.status_code}")

    except Exception as e:
        print(f"Error: {e}\nFailed to get token from API. Status code: {response.status_code}")
