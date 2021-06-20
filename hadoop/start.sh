#!/usr/bin/env bash

if [ -z "$CLUSTER_NAME" ]; then
    echo "Starting datanode"
    hdfs --config $HADOOP_CONF_DIR datanode
else
    echo "Starting namenode"
    echo "Y" | hdfs --config $HADOOP_CONF_DIR namenode -format -clusterID CID-YYY
    hdfs --config $HADOOP_CONF_DIR namenode
fi
