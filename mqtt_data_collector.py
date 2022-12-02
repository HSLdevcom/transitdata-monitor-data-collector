import paho.mqtt.client as mqtt
import time
import json
from datetime import datetime
from threading import Thread
import os
from dotenv import load_dotenv
from send_data_to_azure_monitor import send_custom_metrics_request
import signal
from contextlib import contextmanager

load_dotenv()

# MQTT keep alive interval
# This needs to be small enough to detect if the connection is down so that message rate will be calculated correctly
MQTT_KEEP_ALIVE_SECS = 5

IS_DEBUG = os.getenv('IS_DEBUG') == "True"

# How long to listen to the topics until we send data to Azure. Should be 60 in production
MONITOR_PERIOD_IN_SECONDS = 60 if IS_DEBUG == False else 20

class Topic:
    is_starting = False # True while connecting to the broker, False when connected or disconnected
    is_running = False
    msg_count = 0

    measuring_started_at = None
    measuring_stopped_at = None

    def __init__(self, topic_address, topic_name, topic_port):
        self.topic_address = topic_address
        self.topic_name = topic_name
        self.topic_port = topic_port

    def get_broker_address(self):
        return f"{self.topic_address}:{self.topic_port}"

    def print_status(self):
        print(f"[Status] {self.topic_name}: msg_count: {self.msg_count}, is_running: {self.is_running}")

    def listen_topic(self):
        """
            Documentation for paho.mqtt.python: https://github.com/eclipse/paho.mqtt.python
        """
        if self.is_starting:
            print(f"MQTT client is already connecting to {self.get_broker_address()}")
            return

        self.is_starting = True

        self.measuring_started_at = None
        self.measuring_stopped_at = None

        client = mqtt.Client()

        client.on_connect = self._on_connect_callback
        client.on_message = self._on_message_callback
        client.on_disconnect = self._on_disconnect_callback

        # Enable debugging if needed
        #client.on_log = self._on_log_callback

        client.connect_async(self.topic_address, int(self.topic_port), MQTT_KEEP_ALIVE_SECS)

        print(f"Connecting to MQTT broker at {self.get_broker_address()}")
        # Starts thread that processes network traffic and dispatches callbacks
        client.loop_start()

    # The callback for when the client receives a CONNACK response from the server.
    def _on_connect_callback(self, client, userdata, flags, rc):
        print(f"Connected to MQTT broker at {self.get_broker_address()}")
        self.is_starting = False
        if rc == 0:
            self.is_running = True
            client.subscribe(self.topic_name)
            self.measuring_started_at = time.perf_counter()
            self.measuring_stopped_at = None
        else:
            print(f"Error on connecting {client}, is our IP whitelisted for the topic?")

    # Called when MQTT is disconnected
    def _on_disconnect_callback(self, client, userdata, rc):
        print(f"Disconnected from {self.topic_address}, rc: {rc}")
        self.measuring_stopped_at = time.perf_counter()
        client.loop_stop()
        self.is_running = False

    # # The callback for when a PUBLISH message is received from the server.
    def _on_message_callback(self, client, userdata, msg):
        self.msg_count += 1
        # print(msg.topic+" "+str(msg.payload))

    def get_msg_count(self):
        if self.measuring_started_at == None:
            print(f"No data was measured for {self.get_broker_address()} on topic {self.topic_name}. Maybe the client was not connected?")
            return None

        if self.measuring_stopped_at != None:
            elapsed_time = self.measuring_stopped_at - self.measuring_started_at

            # If data was collected for too short period, we can't accurately calculate the message rate
            if elapsed_time < min(25, 10*MQTT_KEEP_ALIVE_SECS):
                # Return None if elapsed_time is too small to calculate accurate result
                return None

            """
                Adjust elapsed_time to account for the time it took to detect that the connection was down.
                This should take roughly 2 times the duration of MQTT keep alive interval.
                This adjustment can cause the message rate to be slightly inflated, but this is less of a problem than too small message rate, which would cause unnecessary alerts.
            """
            elapsed_time -= 2*MQTT_KEEP_ALIVE_SECS
        else:
            elapsed_time = time.perf_counter() - self.measuring_started_at

        if IS_DEBUG:
            print(f"started: {self.measuring_started_at}, stopped: {self.measuring_started_at + elapsed_time}")
            print(f"Elapsed time {elapsed_time}, messages: {self.msg_count}")

        msg_per_second = self.msg_count / elapsed_time
        self.msg_count = 0
        self.measuring_started_at = time.perf_counter()
        self.measuring_stopped_at = None

        return msg_per_second

    # Enable debugging if needed
    # def _on_log_callback(self, client, userdata, level, buf):
        # print(buf)

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

    for topic in topic_list:
        topic.listen_topic()

    time_end = time.perf_counter() + MONITOR_PERIOD_IN_SECONDS
    # Keep listening to topics forever
    while True:
        sleep_time = time_end - time.perf_counter()
        print(f"Sleeping for {sleep_time} secs")

        # Only sleep if sleep_time is positive. This can happen if sending data to Azure took longer than MONITOR_PERIOD_IN_SECONDS
        if sleep_time > 0:
            # Sleep while listen period is going, after that we send data to Azure
            time.sleep(sleep_time)

        # TODO: remove this logging later when not needed
        # print("After sleep.")

        # Set time_end as MONITOR_PERIOD_IN_SECONDS in the future
        time_end = time.perf_counter() + MONITOR_PERIOD_IN_SECONDS
        topic_data_map = {}

        # Save message counters into topic_data_map and reset them in each topic
        for topic in topic_list:
            topic_data_map_key = f"{topic.topic_address}:{topic.topic_name}:{topic.topic_port}"
            topic_data_map_value = topic.get_msg_count()
            if topic_data_map_value != None:
                topic_data_map[topic_data_map_key] = topic_data_map_value

        t = Thread(target=send_mqtt_msg_count_to_azure, args=(topic_data_map,))
        t.start()

        # (Re)start threads that are in is_running == False state
        for topic in topic_list:
            if topic.is_running == False:
                print(f"Topic {topic.topic_name} was not running, starting it.")

                topic.listen_topic()


def send_mqtt_msg_count_to_azure(topic_data_map):
    """
    Send custom metrics into azure. Documentation for the required format can be found from here:
    https://docs.microsoft.com/en-us/azure/azure-monitor/essentials/metrics-custom-overview
    # Subject: which Azure resource ID the custom metric is reported for.
    # Is included in the URL of the API call
    # Region: must be the same for the resource ID and for log analytics
    # Is included in the URL of the API call
    """

    # Azure wants time in UTC ISO 8601 format
    time_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    series_array = get_series_array(topic_data_map)
    if not series_array:
        print("No data to send to Azure")
        return

    custom_metric_object = {
        # Time (timestamp): Date and time at which the metric is measured or collected
        "time": time_str,
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
        # Try sending data to Azure multiple times, wait between attempts
        is_ok = send_custom_metrics_request(custom_metric_json=custom_metric_json, attempts_remaining=3)
        if is_ok == False:
            print("Sending data to Azure failed, trying again in 5 minutes.")
            time.sleep(300) # Wait 5 minutes before the next attempt
            is_ok = send_custom_metrics_request(custom_metric_json=custom_metric_json, attempts_remaining=3)
            if is_ok == False:
                print("Sending data to Azure failed, trying again in 10 minutes.")
                time.sleep(600) # Wait 10 minutes before the next attempt
                is_ok = send_custom_metrics_request(custom_metric_json=custom_metric_json, attempts_remaining=3)

        if is_ok:
            print(f"Mqtt metrics sent: {datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}")
        else:
            print("Failed to send metrics to Azure.")

def get_series_array(topic_data_map):
    series_array = []
    for key in topic_data_map:
        topic_msg_count = topic_data_map[key]

        topic_msg_count = round(topic_msg_count, 2)

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
