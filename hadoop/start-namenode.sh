#!/usr/bin/env bash

echo "Y" | hdfs namenode -format -clusterID CID-YYY
hdfs namenode
