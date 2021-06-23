# Cartolafc reports

<p>
<img alt="Docker" src="https://img.shields.io/badge/docker-%230db7ed.svg?&style=for-the-badge&logo=docker&logoColor=white"/>
<img alt="Apache Airflow" src="https://img.shields.io/badge/apacheairflow-%23017cee.svg?&style=for-the-badge&logo=apache-airflow&logoColor=white"/>
<img alt="Apache Hive" src="https://img.shields.io/badge/apachehive-%23FDEE21.svg?&style=for-the-badge&logo=apache-hive&logoColor=white"/>
<img alt="Apache Spark" src="https://img.shields.io/badge/apachespark-%23e25a1c.svg?&style=for-the-badge&logo=apache-spark&logoColor=white"/>
</p>

- Hadoop Web UI: http://localhost:9870
- Spark Web UI: http://localhost:8000
- Airflow Web UI: http://localhost:8080

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
