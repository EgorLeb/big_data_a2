#!/bin/bash

apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install cqlsh

echo "Waiting for Cassandra to start..."
until cqlsh cassandra-server -e 'DESCRIBE KEYSPACES'; do
  sleep 20
  echo "retrying to connect"
done

echo "Cassandra is ready. Initializing schema..."
cqlsh cassandra-server -f /app/cassandra/cassandra-init.cql
