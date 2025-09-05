## Apache Kafka Connector for Moonlink
This is the testbed for building a Kafka Connector into [moonlink](https://github.com/Mooncake-Labs/moonlink). This is adapted from AWS' kafka connector demo.

### Requirements

* [Java 11+](https://openjdk.org/install)
* [Maven 3.8.6+](https://maven.apache.org/download.cgi)
* [Docker](https://www.docker.com/get-started)

<!-- for AWS deployment -->
* [Terraform 1.3.0+](https://www.terraform.io/downloads)
* [AWS Account](https://aws.amazon.com/resources/create-account)
* [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

## ⚙️ Building the connector

The first thing you need to do to use this connector is to build it.

1. Install the following dependencies:

- [Java 11+](https://openjdk.java.net)
- [Apache Maven](https://maven.apache.org)

2. Build the Kafka Connect connector file.

```bash
mvn clean package
```

💡 A file named `target/moonlink-source-connector-1.0.jar` will be created. This is your connector for Kafka Connect.

## ⬆️ Starting the local environment

With the connector properly built, you need to have a local environment to test it. This project includes a Docker Compose file that can spin up container instances for Apache Kafka and Kafka Connect.

1. Install the following dependencies:

- [Docker](https://www.docker.com/get-started)

2. Start the containers using Docker Compose.

```bash
scripts/rebuild-connect.sh
```

or:

```bash
docker compose up -d
```

Wait until the containers `kafka` and `connect` are started and healthy.

## VScode syntax highlighting

If the IDE is not picking up the protobuf created files, we want to re-compile and ensure that the java extension workspace is refreshed. 

```
mvn -q -DskipTests=true clean generate-sources && mvn -q -DskipTests=true compile
```

In vscode / cursor command palette (Cmd shit p): `Java: Clean Java Language Server Workspace` or `Java: Force Java project update`

## ⏯ Deploying and testing the connector

Nothing is actually happening since the connector hasn't been deployed. Once you deploy the connector, it will start generating sample data from an artificial source and write this data off into three Kafka topics.

1. Deploy the connector.

```bash
curl -X POST -H "Content-Type:application/json" -d @examples/moonlink-sink-connector.json http://localhost:8083/connectors

curl -X POST -H "Content-Type:application/json" -d @examples/example-source-connector.json http://localhost:8083/connectors
```

2. Check if the connector is producing data to Kafka topics.

### Viewing logs:
docker compose exec kafka bash

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic source-1 --from-beginning
```

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic source-2 --from-beginning
```

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic source-3 --from-beginning
```

💡 All three topics should have sample data continuously generated for them.

You can view the logs using 
```bash
docker compose logs -f connect

# Check status
curl -s http://localhost:8083/connectors/moonlink-sink-connector/status | jq -C . | cat

# Check logs of moonlink source
docker compose logs -t --since=10m connect | grep -i -E "Source task stats|stopped sending"

# Check logs of moonlink sink
docker compose logs -t --since=30m connect | grep -i "Moonlink Sink Task" | tail -n 200 | cat

docker compose logs -t --since=10m connect | grep -i "Sink task stats" | tail -n 100 | cat

scripts/run-datafusion.sh
SELECT COUNT(*) FROM mooncake."test_db"."test_table";

# check possible errors
docker compose logs -t --since=2h connect | grep -i -E "moonlink|Moonlink|moonlink.sink|MoonlinkSink" -C2 | tail -n 200 | cat

curl -sS http://localhost:3030/tables | jq -C .
```

## 🪲 Debugging the connector

This is actually an optional step, but if you wish to debug the connector code to learn its behavior by watching the code executing line by line, you can do so by using remote debugging. The Kafka Connect container created in the Docker Compose file was changed to rebind the port **8888** to enable support for [JDWP](https://en.wikipedia.org/wiki/Java_Debug_Wire_Protocol). The instructions below assume that you are using [Visual Studio Code](https://code.visualstudio.com) for debugging. However, most IDEs for Java should provide support for JDWP. Please check their documentation manuals about how to attach their debugger to the remote process.

1. Create a file named `.vscode/launch.json` with the following content:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Debug Connector",
            "type": "java",
            "request": "attach",
            "hostName": "localhost",
            "port": 8888
        }
    ]
}
```

2. Set one or multiple breakpoints throughout the code.
3. Launch a new debugging session to attach to the container.
4. Play with the connector to trigger the live debugging.

## ⏹ Undeploy the connector

Use the following command to undeploy the connector from Kafka Connect:

```bash
curl -X DELETE http://localhost:8083/connectors/my-first-kafka-connector
```

## ⬇️ Stopping the local environment

1. Stop the containers using Docker Compose.

```bash
docker compose down
```

# From AWS Demo:

## 🌩 Deploying into AWS

Once you have played with the connector locally, you can also deploy the connector in the cloud. This project contains the code necessary for you to automatically deploy this connector in AWS using Terraform. To deploy the connector in AWS, you will need:

- [Terraform 1.3.0+](https://www.terraform.io/downloads)
- [AWS Account](https://aws.amazon.com/resources/create-account)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

You will also need to have the credentials from your AWS account properly configured in your system. You can do this by running the command `aws configure` using the AWS CLI. More information on how to do this [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html).

Follow these steps to execute the deployment.

1. Go to the `deploy-aws` folder.

```bash
cd deploy-aws
```

2. Initialize the Terraform plugins.

```bash
terraform init
```

3. Execute the deployment.

```bash
terraform apply -auto-approve
```

It may take several minutes for this deployment to finish, depending on your network speed, AWS region selected, and other factors. On average, you can expect something like **45 minutes**.

🚨 Please note that the Terraform code will create **35 resources** in your AWS account. It includes a VPC, subnets, security groups, IAM roles, CloudWatch log streams, an S3 bucket, a MSK cluster, an MSK Connect instance, and one EC2 instance. For this reason, be sure to execute the ninth step to destroy these resources, so you don't end up with an unexpected bill.

Once the deployment completes, you should see the following output:

```bash
Outputs:

execute_this_to_access_the_bastion_host = "ssh ec2-user@<PUBLIC_IP> -i cert.pem"
```

4. SSH into the bastion host.

```bash
ssh ec2-user@<PUBLIC_IP> -i cert.pem
```

💡 The following steps assume you are connected to the bastion host.

5. List the Kafka endpoints stored in the `/home/ec2-user/bootstrap-servers` file.

```bash
more /home/ec2-user/bootstrap-servers
```

6. Copy one of the endpoints shown from the command above.

7. Check if the connector is writing data to the topics.

```bash
kafka-console-consumer.sh --bootstrap-server <ENDPOINT_COPIED_FROM_STEP_SIX> --topic source-1 --from-beginning
```

```bash
kafka-console-consumer.sh --bootstrap-server <ENDPOINT_COPIED_FROM_STEP_SIX> --topic source-2 --from-beginning
```

```bash
kafka-console-consumer.sh --bootstrap-server <ENDPOINT_COPIED_FROM_STEP_SIX> --topic source-3 --from-beginning
```

💡 All three topics should have sample data continuously generated for them.

8. Exit the connection with the bastion host.

```bash
exit
```

9. Destroy all the resources created by Terraform.

```bash
terraform destroy -auto-approve
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the MIT-0 License. See the [LICENSE](./LICENSE) file.
