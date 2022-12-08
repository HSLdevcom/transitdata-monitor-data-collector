# Transitdata Monitor Data Collector

Collects MQTT and Pulsar data and sends it to Azure Monitor so that alerts can monitor the data and alert when needed.

## Run locally

Have `.env` file at the project directory containing all of the secret values (you can get secrets from a pulsar-proxy VM from pulsar-dev resource group)

### Run pulsar data collector

To run `pulsar_data_collector.py`, you need to have a tunnel open to pulsar_dev_proxy so that `ADMIN_URL` env variable points to pulsar admin's port.

Also make sure that NAMESPACE value is correct in .env

and then run either:
```
python3 pulsar_data_collector.py
```

### Run mqtt data collector

To run `mqtt_data_collector.py`, some of the addresses might require having a tunnel open to pulsar_bastion and then listening through the tunnel.

Example of opening two tunnels into localhost ports 9001 and 9002:

```
ssh -L 9001:<topic_1_address>:<topic_2_address> -L 9002:<topic_1_address>:<topic_2_address> <pulsar_bastion_private_ip>
```

and in `.env` file, you need to have topics that require a tunnel configured like so: TOPIC<topic_index>=localhost,<topic_name>,9001
for those topics that require tunneling.

Now you can run:
```
python3 mqtt_data_collector.py
```

### Run GTFS-RT data collector

Add list of GTFS-RT URLs to an environment variable named `GTFSRT_URLS` and run:

```bash
python3 gtfsrt_data_collector.py
```

## Deployment

Deployment is done with ansible on the pulsar proxy server. In order to update this app, create a new release in github: https://github.com/HSLdevcom/transitdata-monitor-data-collector/releases/new and then run the pulsar proxy playbook.
