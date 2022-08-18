import paho.mqtt.client as mqtt
import time
import json
from datetime import datetime
from threading import Thread
import os
from dotenv import load_dotenv
from send_data_to_azure_monitor import send_custom_metrics_request

load_dotenv()

IS_DEBUG = os.getenv('IS_DEBUG') == "True"

# How long to listen to the topics until we send data to Azure. Should be 60 in production
MONITOR_PERIOD_IN_SECONDS = 60 if IS_DEBUG == False else 3

def main():
    """
        In order for this to work, info for each topic (IP address, topic name and port) has to be defined in .env file with format: TOPIC<topic index>=<IP address, topic name, port>

        Creates a thread for each topic to listen
        After listening to all the threads is finished, send data to Azure Monitor
    """
    topic_data_collection = {}
    index = 1
    threads = []
    while True:
        topic_data_string = os.getenv(f'TOPIC{index}')
        index += 1
        if (topic_data_string is None):
            break

        def listen_topic_thread(topic_data_string):
            if topic_data_string is None or topic_data_string.count(',') != 2:
                raise Exception(
                    f"Some topic data was missing. Required data: address,topic,port. We got: {topic_data_string}")
            topic_data_array = topic_data_string.split(",")
            topic_address = topic_data_array[0]
            topic_name = topic_data_array[1]
            topic_port = topic_data_array[2]
            if (topic_address is None or topic_name is None or topic_port is None):
                raise Exception(f"Some required topic data was missing, topic_address: {topic_address}, topic_name: {topic_name}, topic_port: {topic_port}")
            topic_data_collection_key = f"{topic_address}:{topic_name}:{topic_port}"
            topic_data_collection[topic_data_collection_key] = 0

            listen_topic(topic_data_collection, topic_data_collection_key, topic_address, topic_name, topic_port)
        t = Thread(target=listen_topic_thread, args=(topic_data_string,))
        threads.append(t)

    # Start all threads simultaneously
    for i in range(len(threads)):
        threads[i].start()

    # Wait for all the threads to finish
    for i in range(len(threads)):
        threads[i].join()

    if IS_DEBUG:
        print(topic_data_collection)
    else:
        send_mqtt_msg_count_into_azure(topic_data_collection)
        print(f'Mqtt metrics sent: {datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}')

def listen_topic(topic_data_collection, topic_data_collection_key, address, topic, port):
    """
        Documentation for paho.mqtt.python: https://github.com/eclipse/paho.mqtt.python
    """
    time_end = time.time() + MONITOR_PERIOD_IN_SECONDS

    client = mqtt.Client()
    client.on_connect = on_connect_callback(topic)
    client.on_message = on_message_callback(topic_data_collection, topic_data_collection_key)
    # Enable debugging if needed
    # client.on_log = on_log_callback
    # client.on_disconnect = on_disconnect_callback

    try:
        client.connect(address, int(port), MONITOR_PERIOD_IN_SECONDS)
    except:
        print(f'Error: could not connect to {address} {topic} {port}')

    # Call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    client.loop_start()

    while time.time() < time_end:
        time.sleep(1)

    client.loop_stop()

# The callback for when the client receives a CONNACK response from the server.
def on_connect_callback(topic):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            client.subscribe(topic)
        else:
            print(f'Error on connecting {client}, is our IP whitelisted for the topic?')
    return on_connect

# # The callback for when a PUBLISH message is received from the server.
def on_message_callback(topic_data_collection, topic_data_collection_key):

    def on_message(client, userdata, msg):
        topic_data_collection[topic_data_collection_key] += 1
        # print(msg.topic+" "+str(msg.payload))

    return on_message

# Enable debugging if needed
# def on_log_callback(client, userdata, level, buf):
    # print(buf)

# Enable debugging if needed
# def on_disconnect_callback(client, userdata, rc):
#     print("Disconnected")

def send_mqtt_msg_count_into_azure(topic_data_collection):
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

    series_array = get_series_array(topic_data_collection)

    custom_metric_object = {
        # Time (timestamp): Date and time at which the metric is measured or collected
        "time": time,
        "data": {
            "baseData": {
                # Metric (name): name of the metric
                "metric": "Msg Count",
                # Namespace: Categorize or group similar metrics together
                "namespace": "MQTT",
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

    send_custom_metrics_request(custom_metric_json, attempts_remaining=3)

def get_series_array(topic_data_collection):
    series_array = []
    for key in topic_data_collection:
        topic_msg_count = topic_data_collection[key]

        # We want message count to be message per second
        topic_msg_count = round(topic_msg_count/MONITOR_PERIOD_IN_SECONDS, 2)

        # Azure doesn't seem to like # in a dimValue, replace it with *
        parsed_key = key.replace("#", "*")
        # Azure doesn't seem to like + in a dimValue, replace it with ^
        parsed_key = parsed_key.replace("+", "^")

        dimValue = {
            "dimValues": [
                parsed_key
            ],
            "sum": topic_msg_count,
            "count": 1
        }
        series_array.append(dimValue)
    return series_array

if __name__ == '__main__':
    main()
