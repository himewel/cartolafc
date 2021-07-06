# Cartolafc reports

<p>
<img alt="Docker" src="https://img.shields.io/badge/docker-%230db7ed.svg?&style=for-the-badge&logo=docker&logoColor=white"/>
<img alt="Apache Airflow" src="https://img.shields.io/badge/apacheairflow-%23017cee.svg?&style=for-the-badge&logo=apache-airflow&logoColor=white"/>
<img alt="Apache Hive" src="https://img.shields.io/badge/apachehive-%23FDEE21.svg?&style=for-the-badge&logo=apache-hive&logoColor=white"/>
</p>

<p align="center">
<img alt="Architecture" src="./architecture.jpg"/>
</p>

This project aims to build and structure a data lake and data warehouse based on the data extracted from Cartola FC (a game about the Brazilian national football championship). The current data warehouse schema used on Hive and the Airflow DAG diagram are presented next:

<p align="center">
<img alt="Database schema" src="./schema.png"/>
</p>

<p align="center">
<img alt="Airflow DAG" src="./dag.png"/>
</p>

## How to start

The base of components of the project is orchestrated at the `docker-compose.yaml` in the root level of the project. While some components are still not integrated, they have their own `docker-compose.yaml` located at each component folder. At the root level, hadoop and airflow are integrated. So, to setup the base project run the following:

```shell
docker-compose up --detach
```

Then, the containers of hadoop, airflow, hive and superset will startup. After some moments of the startup of the services, you can check the web interfaces:
- Hadoop Web UI: http://localhost:9870
- Airflow Web UI: http://localhost:8080
- Superset Web UI: http://localhost:8088

At the folder `notebooks` you can see the exploratory analysis of the data separate by each year of the historic data. The code developed there was used to form the classes of transforms implemented with the factoring `airflow/include/transform_factory.py`.
