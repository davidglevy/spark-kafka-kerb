#!/bin/bash

mvn clean install
cd target
unzip spark-kafka-kerb-bin.zip
cd spark-kafka-kerb
bin/run-example-consumer.sh
