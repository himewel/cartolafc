#!/usr/bin/env bash

namenode_dir=$(ls -A /hadoop/dfs/name)
if [[ ! -z "$namenode_dir" ]]; then
    echo "Directory is NOT empty!"
else
    echo "Directory is empty!"
    echo "Y" | hdfs namenode -format -clusterID CID-YYY
fi

hdfs namenode
