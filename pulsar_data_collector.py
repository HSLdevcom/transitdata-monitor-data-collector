import requests
import json
from datetime import datetime
import os
from dotenv import load_dotenv
from send_data_to_azure_monitor import send_custom_metrics_request

load_dotenv()

IS_DEBUG = os.getenv('IS_DEBUG') == "True"
ADMIN_URL=os.getenv('ADMIN_URL')
NAMESPACE=os.getenv('NAMESPACE')

METRIC_MSG_RATE_IN = "Msg Rate In"
METRIC_MSG_RATE_OUT = "Msg Rate Out"
METRIC_STORAGE_SIZE = "Storage Size"

TOPIC_NAMES_TO_COLLECT_MSG_RATE_IN = [
    "hfp-mqtt-raw/v2",
    "hfp-mqtt-raw-deduplicated/v2",
    "hfp/v2",
    "gtfs-rt/feedmessage-vehicleposition",
    "metro-ats-mqtt-raw/metro-estimate",
    "metro-ats-mqtt-raw-deduplicated/metro-estimate",
    "source-metro-ats/metro-estimate",
    "source-pt-roi/arrival",
    "source-pt-roi/departure",
    "internal-messages/pubtrans-stop-estimate",
    "internal-messages/feedmessage-tripupdate",
    "gtfs-rt/feedmessage-tripupdate",
    "internal-messages/stop-cancellation"
]

TOPIC_NAMES_TO_COLLECT_MSG_RATE_OUT = [
    "hfp/passenger-count",
    "gtfs-rt/feedmessage-vehicleposition",
    "gtfs-rt/feedmessage-tripupdate"
]

TOPIC_NAMES_TO_COLLECT_STORAGE_SIZE = [
    "hfp/v2"
]


def main():
    topic_data_collection = {}
    # Merge all topic name lists as a single array
    collect_data_from_topics_list = list(set(TOPIC_NAMES_TO_COLLECT_MSG_RATE_IN + TOPIC_NAMES_TO_COLLECT_MSG_RATE_OUT + TOPIC_NAMES_TO_COLLECT_STORAGE_SIZE))

    for topic_name in collect_data_from_topics_list:
        topic_data = collect_data_from_topic(topic_name)
        if topic_data != None:
            topic_data_collection[topic_name] = topic_data

    if bool(topic_data_collection):
        send_metrics_into_azure(topic_data_collection)
    else:
        print(f'Not sending metrics, topic_data_collection was empty.')

def collect_data_from_topic(topic_name):
    pulsar_url = f'{ADMIN_URL}/admin/v2/persistent/{NAMESPACE}/{topic_name}/stats'
    try:
        r = requests.get(url=pulsar_url)
        topic_data = r.json()
        # print(f'Stats of topic {topic_data}:')
        # print(f'{topic_data["msgRateIn"]}')
        # print(f'{topic_data["msgRateOut"]}')
        # print(f'{topic_data["storageSize"]}')
        return topic_data
    except Exception as e:
        print(f'Failed to send a POST request to {pulsar_url}. Is pulsar running and accepting requests?')

def send_metrics_into_azure(topic_data_collection):
    send_pulsar_topic_metric_into_azure(METRIC_MSG_RATE_IN, "msgRateIn", topic_data_collection, TOPIC_NAMES_TO_COLLECT_MSG_RATE_IN)
    send_pulsar_topic_metric_into_azure(METRIC_MSG_RATE_OUT, "msgRateOut", topic_data_collection, TOPIC_NAMES_TO_COLLECT_MSG_RATE_OUT)
    send_pulsar_topic_metric_into_azure(METRIC_STORAGE_SIZE, "storageSize", topic_data_collection, TOPIC_NAMES_TO_COLLECT_STORAGE_SIZE)
    print(f'Pulsar metrics sent: {datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}')

def send_pulsar_topic_metric_into_azure(
        log_analytics_metric_name,
        topic_data_metric_name,
        topic_data_collection,
        topic_names_to_collect
):
    """
    Send custom metrics into azure. Documentation for the required format can be found from here:
    https://docs.microsoft.com/en-us/azure/azure-monitor/essentials/metrics-custom-overview
    # Subject: which Azure resource ID the custom metric is reported for.
    # Is included in the URL of the API call
    # Region: must be the same for the resource ID and for log analytics
    # Is included in the URL of the API call
    """

    # Azure wants time in UTC ISO 8601 format
    time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    series_array = get_series_array(topic_data_collection, topic_data_metric_name, topic_names_to_collect)

    custom_metric_object = {
        # Time (timestamp): Date and time at which the metric is measured or collected
        "time": time,
        "data": {
            "baseData": {
                # Metric (name): name of the metric
                "metric": log_analytics_metric_name,
                # Namespace: Categorize or group similar metrics together
                "namespace": "Pulsar",
                # Dimension (dimNames): Metric has a single dimension
                "dimNames": [
                  "Topic"
                ],
                # Series: data for each monitored topic
                "series": series_array
            }
        }
    }

    custom_metric_json = json.dumps(custom_metric_object)

    if IS_DEBUG:
        print(custom_metric_json)
    else:
        send_custom_metrics_request(custom_metric_json, 3)

def get_series_array(topic_data_collection, topic_data_metric_name, topic_names_to_collect):
    series_array = []
    for topic_name in topic_names_to_collect:
        topic_msg_count = topic_data_collection[topic_name][topic_data_metric_name]
        # Special case: we want to multiply stop-cancellation messages by 10
        # so that the data would show more likely in Azure's charts
        if topic_name == "internal-messages/stop-cancellation":
            topic_msg_count = topic_msg_count * 10

        dimValue = {
            "dimValues": [
                topic_name
            ],
            "sum": round(topic_msg_count),
            "count": 1
        }
        series_array.append(dimValue)
    return series_array

if __name__ == '__main__':
    main()