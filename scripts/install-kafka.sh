# install kafka


# create client.properties
printf 'security.protocol=SASL_SSL\nsasl.mechanism=AWS_MSK_IAM\nsasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;\nsasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler\n' | sudo tee /opt/kafka/config/client.properties >/dev/null

set -e; KAFKA_VERSION=3.8.0; if [ -f /opt/kafka_2.13-${KAFKA_VERSION}.tgz ]; then echo "Found /opt/kafka_2.13-${KAFKA_VERSION}.tgz"; else echo "Tarball not found yet"; fi; ls -lh /opt/kafka_2.13-${KAFKA_VERSION}.tgz 2>/dev/null || true; if [ -f /opt/kafka_2.13-${KAFKA_VERSION}.tgz ]; then sudo tar -xzf /opt/kafka_2.13-${KAFKA_VERSION}.tgz -C /opt && sudo ln -sfn /opt/kafka_2.13-${KAFKA_VERSION} /opt/kafka; fi; sudo mkdir -p /opt/kafka/libs /opt/kafka/config; if [ ! -f /opt/kafka/libs/aws-msk-iam-auth-all.jar ]; then sudo wget -q -O /opt/kafka/libs/aws-msk-iam-auth-all.jar https://repo1.maven.org/maven2/software/amazon/msk/aws-msk-iam-auth/2.0.3/aws-msk-iam-auth-2.0.3-all.jar; fi; printf 'security.protocol=SASL_SSL\nsasl.mechanism=AWS_MSK_IAM\nsasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;\nsasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler\n' | sudo tee /opt/kafka/config/client.properties >/dev/null; ls -l /opt/kafka | cat

# create source-1 topic
CLASSPATH=/opt/kafka/libs/aws-msk-iam-auth-all.jar /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server boot-tp9wixno.c3.kafka-serverless.us-east-1.amazonaws.com:9098 --command-config /opt/kafka/config/client.properties --replication-factor 3 --partitions 1 --topic source-1 | cat

# list all topics
CLASSPATH=/opt/kafka/libs/aws-msk-iam-auth-all.jar /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server boot-tp9wixno.c3.kafka-serverless.us-east-1.amazonaws.com:9098 --command-config /opt/kafka/config/client.properties | cat

# delete all amazon_msk_connect_ topics
export CLASSPATH=/opt/kafka/libs/aws-msk-iam-auth-all.jar; BOOT=boot-tp9wixno.c3.kafka-serverless.us-east-1.amazonaws.com:9098; CONFIG=/opt/kafka/config/client.properties; /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server "$BOOT" --command-config "$CONFIG" | grep 'amazon_msk_connect_' | xargs -r -n1 -I{} /opt/kafka/bin/kafka-topics.sh --delete --bootstrap-server "$BOOT" --command-config "$CONFIG" --topic {}

# set environment variables
BOOT=boot-tp9wixno.c3.kafka-serverless.us-east-1.amazonaws.com:9098
CFG=/opt/kafka/config/client.properties
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"
export CLASSPATH=/opt/kafka/libs/aws-msk-iam-auth-all.jar

# delete source-1 topic
export CLASSPATH=/opt/kafka/libs/aws-msk-iam-auth-all.jar; BOOT=boot-tp9wixno.c3.kafka-serverless.us-east-1.amazonaws.com:9098; CONFIG=/opt/kafka/config/client.properties; /opt/kafka/bin/kafka-topics.sh --delete --bootstrap-server "$BOOT" --command-config "$CONFIG" --topic source-1