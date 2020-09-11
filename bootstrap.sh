#!/bin/bash

echo "# To kill the bootstrapped servers:"
zookeeper-server-start config/zookeeper.properties > zookeeper.log &
echo "kill -9 $! # zookeeper server"
sleep 5
kafka-server-start config/server.properties > kafka.log &
echo "kill -9 $! # kafka server"
