#!/bin/bash

cd server/target/
tar -xzf pod-map-reduce-server-1.0-SNAPSHOT-bin.tar.gz
cd pod-map-reduce-server-1.0-SNAPSHOT/
chmod 777 run-server.sh
cd ..
cd ..
cd ..