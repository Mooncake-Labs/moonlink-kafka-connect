#!/bin/sh

##########################################################################
################################ Important ###############################
##########################################################################
##  This script implements workarounds for the current Docker image of  ##
##  Apache Kafka from Confluent. Eventually, newer images will fix the  ##
##  issues found here, and this script will no longer be required.      ##
##########################################################################

# KRaft: Generate/export CLUSTER_ID at runtime if not provided, then format
if [ -z "$CLUSTER_ID" ]; then
    CLUSTER_ID="$(kafka-storage random-uuid)"
    echo "Generated CLUSTER_ID=$CLUSTER_ID"
fi
export CLUSTER_ID

# Format storage if needed (idempotent)
kafka-storage format --ignore-formatted -t "$CLUSTER_ID" -c /etc/kafka/kafka.properties
