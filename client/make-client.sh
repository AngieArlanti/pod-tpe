#!/bin/bash

mvn clean install
cd target/
tar -xzf pod-map-reduce-client-1.0-SNAPSHOT-bin.tar.gz
cd pod-map-reduce-client-1.0-SNAPSHOT/
chmod 777 run-client.sh