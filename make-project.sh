#!/bin/bash

mvn clean install

chmod 777 ./server/make-server.sh
chmod 777 ./client/make-client.sh

./server/make-server.sh
./client/make-client.sh