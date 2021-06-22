# Cartola FC reports

- Hadoop Web UI: http://localhost:9870
- Spark Web UI: http://localhost:8080

## How to start

Each folder has a `docker-compose.yaml` file that orchestrates the containers of your respective service. So, for example, to build and get up the hadoop containers:

```shell
docker-compose \
    --file hadoop/docker-compose.yaml \
    up --detach    
```

At the same model, to get up the spark containers (hadoop its a dependencie) run:

```shell
docker-compose \
    --file spark/docker-compose.yaml \
    up --detach    
```

Finally, to get up the hive containers (again, hadoop its a dependencie) run:

```shell
docker-compose \
    --file hive/docker-compose.yaml \
    up --detach    
```
