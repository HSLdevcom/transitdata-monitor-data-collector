from google.transit import gtfs_realtime_pb2
import requests
import time
import json
from datetime import datetime
import os
from dotenv import load_dotenv
from send_data_to_azure_monitor import send_custom_metrics_request

load_dotenv()

IS_DEBUG = os.getenv('IS_DEBUG') == "True"

def get_stats(url):
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(url)
    feed.ParseFromString(response.content)

    num_entities = len(feed.entity)
    time_diff = round(time.time()) - feed.header.timestamp

    return (num_entities, time_diff)

def send_data_to_azure_monitor(time, metric, url, value):
    time_str = time.strftime("%Y-%m-%dT%H:%M:%S")

    custom_metric_object = {
        # Time (timestamp): Date and time at which the metric is measured or collected
        "time": time_str,
        "data": {
            "baseData": {
                # Metric (name): name of the metric
                "metric": metric,
                # Namespace: Categorize or group similar metrics together
                "namespace": "GTFSRT",
                # Dimension (dimNames): Metric has a single dimension
                "dimNames": [
                  "URL"
                ],
                # Series: data for each monitored topic
                "series": [
                    {
                        "dimValues": [
                            url
                        ],
                        "sum": value,
                        "count": 1
                    }
                ]
            }
        }
    }

    custom_metric_json = json.dumps(custom_metric_object)

    if IS_DEBUG:
        print(custom_metric_json)
    else:
        send_custom_metrics_request(custom_metric_json, 3)

def main():
    urls = os.getenv("GTFSRT_URLS").split(",")
    for url in urls:
        (entity_count, last_published_ago_secs) = get_stats(url)

        time = datetime.utcnow()

        send_data_to_azure_monitor(time, "Entity Count", url, entity_count)
        send_data_to_azure_monitor(time, "Timestamp Age", url, last_published_ago_secs)

if __name__ == '__main__':
    main()