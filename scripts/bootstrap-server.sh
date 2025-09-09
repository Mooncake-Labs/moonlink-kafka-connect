# 1) Download the IAM auth jar (once)
curl -L -o aws-msk-iam-auth-2.3.0-all.jar \
  https://github.com/aws/aws-msk-iam-auth/releases/download/v2.3.0/aws-msk-iam-auth-2.3.0-all.jar

# 2) Make sure the Kafka bin folder is on PATH and add the jar to the classpath
export KAFKA_HOME=/path/to/your/kafka      # <- adjust
export PATH="$KAFKA_HOME/bin:$PATH"
export CLASSPATH="$KAFKA_HOME/libs/*:$(pwd)/aws-msk-iam-auth-2.3.0-all.jar"

# 3) Create client.properties (IAM auth)
cat > client.properties <<'EOF'
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
EOF

# 4) Use the bootstrap you just retrieved
BS="boot-tp9wixno.c3.kafka-serverless.us-east-1.amazonaws.com:9098"