# Transitdata Monitor Data Collector

Collects MQTT and Pulsar data and sends it to Azure Monitor so that alerts can monitor the data and alert when needed.

### Run locally

To run `pulsar_data_collector.py`, you need to have a tunnel open to pulsar_dev_proxy so that `ADMIN_URL` env variable points to pulsar admin's port.

Have `.env` file at the project directory containing all of the secret values (you can get secrets from a pulsar-proxy VM from pulsar-dev resource group)
and then run either:
```
python3 pulsar_data_collector.py
```
or
```
python3 mqtt_data_collector.py
```

### Send custom metrics manually to Azure Monitor

If you need to send new custom metrics to Azure Monitor,
you can firstly test sending by editing
`custom_metric_example.json` and running:
```
curl -X POST https://westeurope.monitoring.azure.com/<resourceId>/metrics -H "Content-Type: application/json" -H "Authorization: Bearer <AccessToken>" -d @custom_metric_example.json
```
Notes:
- Edit what you need in `custom_metric_example.json` (at least the timestamp)
- You need a fresh `access token` for this command, you can get it by running `main.py` locally (see access_token.txt file)

### Deployment

Deployment is done with ansible on the pulsar proxy server. In order to update this app, create a new release in github: https://github.com/HSLdevcom/transitdata-monitor-data-collector/releases/new and then run the pulsar proxy playbook.
