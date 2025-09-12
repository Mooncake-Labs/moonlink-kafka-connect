This guide shows how to use the Apache Kafka CLI tools to manage topics (create/list/delete) on an Amazon MSK cluster (including MSK Serverless) using AWS IAM authentication (required for serverless MSK).

At a high level:

- Kafka CLI tools (Java) run on your machine.
- The CLI connects to Amazon MSK over TLS/SASL using IAM credentials.
- You point the CLI at your bootstrap brokers (an endpoint returned by the AWS CLI).
- You run kafka-topics.sh (etc.) to administer topics.

Prerequisites
- Java 11+ available on PATH.
- Apache Kafka client tools downloaded locally (we only need the CLI, not a Kafka broker).
- Network access to the MSK cluster (typically from within the same VPC/subnet or over VPN/TGW).
- An IAM principal with permissions to administer topics on the cluster.
- AWS CLI v2 configured (aws configure or environment variables).

AWS’s “Create a topic” guide explicitly starts by installing the Kafka client tools on the client host.

### Install the Kafka CLI tools

Download a Kafka release (any 2.13/3.x is fine; examples assume KAFKA_HOME points to the extracted folder):

```
#  Example: Kafka 3.8.0, Scala 2.13
curl -O https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz
tar -xzf kafka_2.13-3.8.0.tgz
export KAFKA_HOME="$PWD/kafka_2.13-3.8.0"
export PATH="$KAFKA_HOME/bin:$PATH"
```

Get the bootstrap brokers (AWS CLI)
1. Find your cluster ARN:

```
aws kafka list-clusters-v2 \
  --query "ClusterInfoList[].{Name:ClusterName,Arn:ClusterArn,Type:ClusterType}" --output table
```

2.	Get its bootstrap brokers:

```
# Full JSON (shows multiple fields)
aws kafka get-bootstrap-brokers --cluster-arn <YOUR_CLUSTER_ARN>

# IAM-enabled bootstrap string only (handy for scripting)
aws kafka get-bootstrap-brokers --cluster-arn <YOUR_CLUSTER_ARN> \
  --query BootstrapBrokerStringSaslIam --output text

```
- Use the BootstrapBrokerStringSaslIam value with IAM auth.
- For MSK Serverless, the returned endpoint typically uses port 9098.

### Add IAM authentication support

Download the MSK IAM auth JAR (the Java plugin that lets the Kafka CLI sign with IAM):

```
# Place this in your Kafka libs directory or any directory you control
curl -L -o aws-msk-iam-auth-2.3.0-all.jar \
  https://github.com/aws/aws-msk-iam-auth/releases/download/v2.3.0/aws-msk-iam-auth-2.3.0-all.jar
```

Set your Java classpath to include the JAR and Kafka’s libs:

```
export CLASSPATH="$KAFKA_HOME/libs/*:$(pwd)/aws-msk-iam-auth-2.3.0-all.jar"
```

This JAR provides the AWS_MSK_IAM SASL mechanism and corresponding callback handler used below.  ￼

### Create client.properties for IAM

Create a file client.properties with:

```
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

This tells the Kafka CLI to use TLS + SASL + IAM with the handlers provided by the JAR.  ￼

### Operate on topics

Set an environment variable for your bootstrap servers:

```
export BOOTSTRAP="<paste output of BootstrapBrokerStringSaslIam here>"
```

Now you can create, list, and delete topics:
```
# Create a topic (example: 1 partition, replication factor 3)
kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config ./client.properties \
  --create --topic your-topic-name --partitions 1 --replication-factor 3

# List topics
kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config ./client.properties \
  --list

# Delete a topic
kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config ./client.properties \
  --delete --topic your-topic-name

```

These are the same steps AWS shows for topic creation—install Kafka tools, then run kafka-topics.sh against your cluster.  ￼

What each moving part does (fundamentals)
- Kafka topic: Named log split into partitions; replication factor copies each partition across brokers for availability.
- Bootstrap brokers: Initial endpoint(s) the client contacts to discover the rest of the cluster.
- Kafka CLI tools: Java-based admin clients shipped with Apache Kafka (e.g., kafka-topics.sh).
- IAM auth JAR: Bridges Kafka SASL to AWS IAM (adds AWS_MSK_IAM mechanism + callback handler).  ￼
- client.properties: Tells the Kafka CLI to use SASL_SSL with AWS_MSK_IAM, loading the classes from the JAR.
- AWS CLI: Returns the bootstrap string via get-bootstrap-brokers, so you don’t hard-code broker addresses.  ￼

### Optional: cleaning up MSK Connect internal topics

If you use MSK Connect (managed Kafka Connect), it creates internal topics (often prefixed). You can delete them with the CLI (be careful—this is destructive and may break running connectors). See MSK Connect docs for how those internal topics are used.  ￼

Troubleshooting
- Connection timeouts → Usually networking: verify your client host can reach the MSK cluster (VPC/subnet/Security Groups/NACLs).
- Auth errors → Ensure your IAM principal is allowed to perform Kafka actions and that the IAM JAR is on the classpath. The required SASL settings are in client.properties.  ￼
- Can’t find callback handler class → The MSK IAM JAR isn’t on the CLASSPATH; add it (and Kafka libs) before running.  ￼
- Which bootstrap string field to use? → For IAM-enabled clusters, prefer BootstrapBrokerStringSaslIam from get-bootstrap-brokers.  ￼

You can try this:
```
export CLASSPATH=/opt/kafka/libs/aws-msk-iam-auth-all.jar; BOOT=$BOOTSTRAP; CONFIG=/opt/kafka/config/client.properties; /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server "$BOOT" --command-config "$CONFIG" | grep 'amazon_msk_connect_' | xargs -r -n1 -I{} /opt/kafka/bin/kafka-topics.sh --delete --bootstrap-server "$BOOT" --command-config "$CONFIG" --topic {}
```

References
- Get bootstrap brokers (CLI)  ￼
- Configure clients for IAM access control (client properties + mechanisms)  ￼
- Create a topic with Kafka tools (AWS MSK guide)  ￼
- MSK IAM auth library (GitHub)  ￼
