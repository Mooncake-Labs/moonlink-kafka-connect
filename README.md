## Apache Kafka Connector for Moonlink
This repository is a fork of [AWS's Building Apache Kafka Connectors](https://github.com/build-on-aws/building-apache-kafka-connectors). It adapts the example to integrate with [Moonlink](https://github.com/Mooncake-Labs/moonlink).

### Requirements
* [Docker](https://www.docker.com/get-started)

All development is recommended to be done in the devcontainer.

## Quick Start

## Understanding the config file
Many scripts in this repository automatically generate REST payloads or kafka connector configurations based off a user-specified config file. It is important to understand this repo-specific config file for fast development.

At the time of writing, the config file is used to both run local integration test loops, and to also build artifacts needed for deployment to MSK.

The config file is a JSON file that contains the following sections:
- `is_local_execution`: whether to run the connector locally or remotely for testing. Not required if just building artifacts for deployment.
- `uris`: the URI of the moonlink instance and the schema registry. Note that the schema registry is required for both local and remote execution and building artifacts, whereas the moonlink instance is only required for remote execution.
- `artifacts.s3`: the S3 bucket and region you want to upload the connector to (only s3 is supported for now). Required for building artifacts.
- `local_moonlink`: configurations for running moonlink locally (only needed if testing locally)
- `source`: the source connector configuration (only needed if using the load generator source connector, keep default values otherwise)
- `sink`: the sink connector configuration. Required for all use cases.
- `table`: the target table configuration. Required for all use cases.


## ⚙️ Building and deploying the connector

### Populate the config file
For now, we only have options to build and deploy the connector to MSK.

The first thing we need to do to use this connector is to build it and upload it to S3. 

First, create a config file named config.json similar to config.example.json. Ensure that the following fields are populated:
- `uris`: both the URI of the moonlink instance and the schema registry
- `artifacts.s3`: the S3 bucket and region you want to upload the connector to (only s3 is supported for now)
- `source`: the source connector configuration if you want to do your own load generation (keep default values otherwise)
- `sink`: the sink connector configuration
- `table`: the table configuration. Will be used to define the target moonlink table to write to, and the schema of the table.

```bash
scripts/build-and-upload-msk-plugin.sh config.json
```

This will build the connector and upload it to S3. It also generates the following local artifacts in the repository:

- `build/msk-plugin/moonlink-loadgen-connector-<version>.zip` : automatically uploaded to S3
- `build/msk-plugin/moonlink-sink-connector-<version>.zip` : automatically uploaded to S3
- `build/configs/source-connector.msk.json` : the source connector configuration for the load generator
- `build/configs/sink-connector.msk.json` : the sink connector configuration
- `build/configs/table-create.moonlink.json` : the JSON payload to create the target table in Moonlink at the create table endpoint


### Create the table in Moonlink (Avro schema)

The generated `build/configs/table-create.moonlink.json` is the payload to create your target table in Moonlink, using the Avro schema provided in your `config.json` under `table.avro_schema`.

Run:
```bash
DB=$(jq -r .table.database config.json)
TBL=$(jq -r .table.name config.json)
MOONLINK=$(jq -r .uris.moonlink config.json)
curl -sS -X POST "${MOONLINK}/tables/${DB}.${TBL}" \
  -H 'content-type: application/json' \
  -d @build/configs/table-create.moonlink.json | jq -C . | cat
```

This must be executed before starting the sink connector so Moonlink has a table to write to. The payload includes `avro_schema` (not a column list), so ensure your `config.json` defines a valid Avro record schema at `table.avro_schema`.

Note that at this point in development, we are still not supporting schema evolution or automatic schema inference, **so creation of a table before kafka ingestion with a valid Avro schema is required**.

### ⏯ Deploying the connector

Note that both steps are applicable for both the source and sink connector.

Step 1: Create a [custom plugin](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect-plugins.html) in MSK

Step 2: Create a [custom connector](https://docs.aws.amazon.com/msk/latest/developerguide/mkc-create-connector-intro.html) in MSK

The connector configurations are already generated in the `build/configs` directory, so there is no need to create them manually.

## ⬆️ Local testing
We have also provided a integration test script to run and test the connector locally. 

Under the hood, this script will:
1. Build the connector using maven
2. Tear down and recreate the neccessary kafka, schema registry, and connect containers
3. Start the moonlink instance locally if `is_local_execution` is true
4. Start the load generator source connector and the moonlink sink connector, and wait for them to be running and healthy
5. Check if the connector is writing data to moonlink and track progress
6. Tabulate correctness and performance metrics.
7. Take a snapshot after the final row is inserted, and cross-check using datafusion (local only) and iceberg (both local and remote)

### Complete the config file

There are two choices for running the integration test:
1. Running everything locally on your machine
2. Connecting to a remote moonlink instance

Note that the following fields are required for both cases:
- `uris.schema_registry` : the URI of your schema registry
- `table` : the target table configuration. Required for all use cases.
- `sink` : the sink connector configuration. Required for all use cases.
- `source` : the source connector configuration (required for load generation in the integration test)

#### Running everything locally on your machine

Set `is_local_execution` to true, and populate the `local_moonlink` section with the path to your moonlink instance.

#### Connecting to a remote moonlink instance

Set `is_local_execution` to false, and populate the `uris.moonlink` with the URI of your moonlink instance and schema registry.

### Run the integration test
Finally, run the following command to start the connector:

```bash
scripts/run-integration-test.sh config.json
```


## ⏯ Other helpful commands

### Manual deployment of the connector:

You can deploy the connector to the local connect instance using the following command:
```bash
curl -X POST -H "Content-Type:application/json" -d @<path_to_sink_connector_config.json> http://localhost:8083/connectors

curl -X POST -H "Content-Type:application/json" -d @<path_to_source_connector_config.json> http://localhost:8083/connectors
```

Check status
```bash
curl -s http://localhost:8083/connectors/moonlink-sink-connector/status | jq -C . | cat
```
You can also delete the connector using the following command:
```bash
curl -X DELETE http://localhost:8083/connectors/my-first-kafka-connector
```

### Reading kafka logs:
Check if the connector is producing data to Kafka topics.

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic source-1 --from-beginning
```

You can view the connect logs using docker compose:
```bash
# Check logs of moonlink source
docker compose logs -t --since=10m connect | grep -i -E "Load generator finished|Load generator stats"

# Check logs of moonlink sink
docker compose logs -t --since=10m connect | grep -i "Sink task batch:"

# check possible errors
docker compose logs -t --since=2h connect | grep -i -E "moonlink|Moonlink|moonlink.sink|MoonlinkSink" -C2
```

## License

This project is licensed under the MIT No Attribution License (MIT-0), consistent with the upstream AWS repository. See the [LICENSE](./LICENSE) file for details.
