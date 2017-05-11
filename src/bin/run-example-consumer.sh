#!/bin/bash

SPARK_KAFKA_VERSION=0.10
export SPARK_KAFKA_VERSION

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
APP_JAR="${DIR}/../spark-kafka-kerb.jar"
echo $APP_JAR
SPARK_JARS=""
FIRST_ITER=true
for x in `ls "${DIR}/../lib/runtime"`
do
  if [ "$FIRST_ITER" = false ] ; then
    SPARK_JARS="${SPARK_JARS},"
  fi
  if [ "$FIRST_ITER" = true ] ; then
    FIRST_ITER=false
  fi
  SPARK_JARS="${SPARK_JARS}${DIR}/../lib/runtime/${x}"
done
echo $SPARK_JARS

spark2-submit --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/home/dlevy/jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf" \
  --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/home/dlevy/jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf" \
  --master yarn --deploy-mode client --jars ${SPARK_JARS} \
  --class com.cloudera.au.demo.ExampleConsumer ${APP_JAR} \
  --brokers toot-nn-1.lab1.com:9092 --groupId demo --topic demo-1 --path /data \
  --appName example-consumer

