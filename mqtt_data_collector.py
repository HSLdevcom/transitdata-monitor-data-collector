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

class Topic:
    is_running = False
    msg_count = 0

    def __init__(self, topic_address, topic_name, topic_port):
        self.topic_address = topic_address
        self.topic_name = topic_name
        self.topic_port = topic_port

    def print_status(self):
        print(f"[Status] {self.topic_name}: msg_count: {self.msg_count}, is_running: {self.is_running}")

    def listen_topic(self):
        """
            Documentation for paho.mqtt.python: https://github.com/eclipse/paho.mqtt.python
        """
        self.is_running = True

        client = mqtt.Client()
        client.on_connect = self._on_connect_callback()
        client.on_message = self._on_message_callback()

        try:
            # Enable debugging if needed
            # client.on_log = on_log_callback
            # client.on_disconnect = on_disconnect_callback

            client.connect(self.topic_address, int(self.topic_port), MONITOR_PERIOD_IN_SECONDS)

            # Call that processes network traffic, dispatches callbacks and
            # handles reconnecting.
            client.loop_start()

        except Exception as e:
            print(f"Error on topic {self.topic_name} {self.topic_address} {self.topic_port}: {e}")
            client.disconnect()
            self.is_running = False

    # The callback for when the client receives a CONNACK response from the server.
    def _on_connect_callback(self):
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                client.subscribe(self.topic_name)
            else:
                print(f"Error on connecting {client}, is our IP whitelisted for the topic?")

        return on_connect

    # # The callback for when a PUBLISH message is received from the server.
    def _on_message_callback(self):
        def on_message(client, userdata, msg):
            self.msg_count += 1
            # print(msg.topic+" "+str(msg.payload))
        return on_message

    # Enable debugging if needed
    # def on_log_callback(self, client, userdata, level, buf):
    # print(buf)

    # Enable debugging if needed
    # def on_disconnect_callback(self, client, userdata, rc):
    #     print("Disconnected")


def main():
    """
        Listens each topic continuously in a thread. Sends topic messages count per second every
        minute to Azure Monitor.

        In order for this to work, info for each topic (IP address, topic name and port) has to
        be defined in .env file with format: TOPIC<topic index>=<IP address, topic name, port>
    """
    print("Starting MQTT topic listener...")

    topic_list = []
    index = 1
    while True:
        topic_data_string = os.getenv(f'TOPIC{index}')
        index += 1
        if (topic_data_string is None):
            break
        if topic_data_string is None or topic_data_string.count(',') != 2:
            raise Exception(
                f"Some topic data was missing. Required data: address,topic,port. We got: {topic_data_string}")
        topic_data_array = topic_data_string.split(",")
        topic_address = topic_data_array[0]
        topic_name = topic_data_array[1]
        topic_port = topic_data_array[2]
        if (topic_address is None or topic_name is None or topic_port is None):
            raise Exception(f"Some required topic data was missing, topic_address: {topic_address}, topic_name: {topic_name}, topic_port: {topic_port}")
        topic = Topic(topic_address, topic_name, topic_port)
        topic_list.append(topic)

    time_end = time.time() + MONITOR_PERIOD_IN_SECONDS
    # Keep listening to topics forever
    while True:
        # (Re)start threads that are in is_running == False state
        for topic in topic_list:
            if topic.is_running == False:
                print(f"Topic {topic.topic_name} was not running, starting it.")
                topic.listen_topic()

        # If listen period has passed, send data to Azure
        if time.time() > time_end:
            time_end = time.time() + MONITOR_PERIOD_IN_SECONDS
            topic_data_map = {}
            for topic in topic_list:
                topic_data_map_key = f"{topic.topic_address}:{topic.topic_name}:{topic.topic_port}"
                topic_data_map[topic_data_map_key] = topic.msg_count
                topic.msg_count = 0
            send_mqtt_msg_count_into_azure(topic_data_map)
        time.sleep(1)

def send_mqtt_msg_count_into_azure(topic_data_map):
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

    series_array = get_series_array(topic_data_map)

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

    if IS_DEBUG:
        print(custom_metric_json)
    else:
        send_custom_metrics_request(custom_metric_json, attempts_remaining=3)
        print(f"Mqtt metrics sent: {datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}")

def get_series_array(topic_data_map):
    series_array = []
    for key in topic_data_map:
        topic_msg_count = topic_data_map[key]

        # We want message count to be messages per second
        topic_msg_count = round(topic_msg_count/MONITOR_PERIOD_IN_SECONDS, 2)

        # If over 10, round to whole number
        if topic_msg_count > 10:
            topic_msg_count = round(topic_msg_count)

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
