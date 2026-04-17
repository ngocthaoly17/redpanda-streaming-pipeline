#!/bin/sh
set -e

/app/wait_for_redpanda.sh redpanda 9092

echo "Lancement du job Spark..."

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.jars.ivy=/home/spark/.ivy2 \
  /app/spark_redpanda_analysis.py