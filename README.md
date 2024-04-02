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

## Run on server

Have `.env` file at the project directory containing all of the secret values (you can get secrets from a pulsar-proxy 
VM from pulsar-dev resource group) and list of topics.

When you have modified the `.env` file, a `monitor-data-collector` service such as `mqttdatacollector` must be restarted.

## Troubleshooting

If Azure Shared dashboard diagrams `transitdata-topic-prod` and/or `transitdata-topic-dev` are not updating (i.e. there 
are dashed lines in the diagrams or the diagrams are showing zero value all the time), you can check log file 
`/var/log/pulsar-data-collector.log` on servers `pulsar-prod-monitor-data-collector` and `pulsar-dev-monitor-data-collector`.

If there are error messages like this in the log files, it probably means that new secrets should be created in Azure 
applications `hsl-pulsar-prod-monitoring-sp` and `hsl-pulsar-dev-monitoring-sp`:
```
Pulsar metrics sent: 2024-01-03T08:02:02
Currently stored access token has expired, getting a new access token.
Request failed for an unknown reason, response: <Response [401]>.
Returning False as sending data to Azure was not successful.
Currently stored access token has expired, getting a new access token.
Request failed for an unknown reason, response: <Response [401]>.
Returning False as sending data to Azure was not successful.
Currently stored access token has expired, getting a new access token.
Request failed for an unknown reason, response: <Response [401]>.
Returning False as sending data to Azure was not successful.
```
New secret values should be updated to files `/opt/monitor-data-collector/.env` on servers 
`pulsar-prod-monitor-data-collector` and `pulsar-dev-monitor-data-collector`. After that there should be new data in 
the Shared dashboard diagrams. I.e. there is no need to restart `monitor-data-collector` services etc.

If there are no error messages in the log files, you can just try to restart a `monitor-data-collector` service such as
`mqttdatacollector`.