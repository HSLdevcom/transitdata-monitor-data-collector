import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

### SECRETS / ENV VARIABLES ###

TENANT_ID=os.getenv('TENANT_ID')
CLIENT_ID=os.getenv('CLIENT_ID')
CLIENT_SECRET=os.getenv('CLIENT_SECRET')
MONITOR_DATA_COLLECTOR_RESOURCE_ID=os.getenv('MONITOR_DATA_COLLECTOR_RESOURCE_ID')
ACCESS_TOKEN_PATH = os.getenv('ACCESS_TOKEN_PATH')

### SECRETS / ENV VARIABLES ###

def send_custom_metrics_request(custom_metric_json, attempts_remaining):
    # Exit if number of attempts reaches zero.
    if attempts_remaining == 0:
        return
    attempts_remaining = attempts_remaining - 1

    make_sure_access_token_file_exists()
    # Create access_token.txt file, if it does not exist
    f = open(ACCESS_TOKEN_PATH, "r")
    existing_access_token = f.read()
    f.close()

    request_url = f'https://westeurope.monitoring.azure.com/{MONITOR_DATA_COLLECTOR_RESOURCE_ID}/metrics'
    headers = {'Content-type': 'application/json', 'Authorization': f'Bearer {existing_access_token}'}
    response = requests.post(request_url, data=custom_metric_json, headers=headers)

    # Return if response is successful
    if response.status_code == 200:
        return

    # Try catch because json.loads(response.text) might not be available
    try:
        response_dict = json.loads(response.text)
        if response_dict['Error']['Code'] == 'TokenExpired':
            print("Currently stored access token has expired, getting a new access token.")
            request_new_access_token_and_write_it_on_disk()
            send_custom_metrics_request(custom_metric_json, attempts_remaining)
        elif response_dict['Error']['Code'] == 'InvalidToken':
            print("Currently stored access token is invalid, getting a new access token.")
            request_new_access_token_and_write_it_on_disk()
            send_custom_metrics_request(custom_metric_json, attempts_remaining)
        else:
            print(f'Request failed for an unknown reason, response: {response_dict}.')
    except Exception as e:
        print(f'Request failed for an unknown reason, response: {response}.')

def make_sure_access_token_file_exists():
    try:
        f = open(ACCESS_TOKEN_PATH, "r")
        f.close()
    except Exception as e:
        # Create access_token.txt file, if it does not exist
        f = open(ACCESS_TOKEN_PATH, "x")
        f.close()

def request_new_access_token_and_write_it_on_disk():
    request_url = f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/token'

    request_data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "resource": "https://monitoring.azure.com/"
    }

    response = requests.post(request_url, data=request_data)
    response_dict = json.loads(response.text)
    new_access_token = response_dict['access_token']

    print("Saving Access token on disk........")
    f = open(ACCESS_TOKEN_PATH, "w")
    f.write(new_access_token)
    f.close()
    print("........Access token saved on disk")
