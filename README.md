# Cartolafc reports

<p>
<img alt="Docker" src="https://img.shields.io/badge/docker-%230db7ed.svg?&style=for-the-badge&logo=docker&logoColor=white"/>
<img alt="Apache Airflow" src="https://img.shields.io/badge/apacheairflow-%23017cee.svg?&style=for-the-badge&logo=apache-airflow&logoColor=white"/>
<img alt="Apache Hive" src="https://img.shields.io/badge/apachehive-%23FDEE21.svg?&style=for-the-badge&logo=apache-hive&logoColor=black"/>
</p>

<p align="center">
<img alt="Architecture" src="./docs/architecture.jpg"/>
</p>

This project aims to build and structure a data lake and data warehouse based on the data extracted from Cartola FC (a game about the Brazilian national football championship).

## How to start

The base of components is orchestrated at the `docker-compose.yaml` in the root level of the project. Services like hadoop datanode and namenode, hive server and metastore, airflow webserver and scheduler, and superset server can be found there. So, to setup the base project run the following:

```shell
docker-compose up --detach
```

Then, the containers of hadoop, airflow, hive and superset will startup. After some moments of the startup of the services, you can check the web interfaces:
- Hadoop Web UI: http://localhost:9870
- Hive Web UI: http://localhost:10002
- Airflow Web UI: http://localhost:8080
- Superset Web UI: http://localhost:8088

## Miscellaneous

The current data warehouse schema used on Hive is presented next. It mirror the trusted layer build on hadoop with external tables (this is the `trusted` schema) to make some ELT to construct the managed tables in the `refined` schema.

<p align="center">
<img alt="Database schema" src="./docs/schema.png"/>
</p>

The Airflow DAG includes tasks of environment setup in hive and hdfs, file extraction from github API and a transform/load groups for each table of the hive schema. The DAG diagram is presented next:

<p align="center">
<img alt="Airflow DAG" src="./docs/dag.png"/>
</p>

Two dashboards are built into superset, the first with an emphasis on teams and the second on player perfomance:

<p align="center">
<img alt="Teams dashboard" src="./docs/dashboard-clubes.png"/>
</p>

<p align="center">
<img alt="Players dashboard" src="./docs/dashboard-atletas.png"/>
</p>
