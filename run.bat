@echo off
docker exec -i namenode bash -c "hdfs dfsadmin -safemode leave"
  docker exec namenode hdfs dfs -rm -r /aggregated_results
  docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /opt/spark-apps/processor.docker.py "120 minutes" "60 minutes" "tumbling" "passenger_count" "3" "VendorID" "trip_distance" "less"

pause