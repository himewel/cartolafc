FROM apache/superset

ENV SUPERSET_HOME /opt/superset
ENV PYTHONPATH "${PYTHONPATH}:${SUPERSET_HOME}"
ENV HADOOP_CONF_DIR "/etc/hive"
ENV HIVE_CONF_DIR "/etc/hive"

ENV SUPERSET_CONFIG_PATH "${SUPERSET_HOME}/superset_config.py"

WORKDIR ${SUPERSET_HOME}

USER root

RUN apt-get update \
    && apt-get install netcat --yes

COPY start-superset.sh .
COPY create-dashboards.sh .
COPY superset_config.py .
COPY lineage_update.py .

RUN chown -R superset ${SUPERSET_HOME} \
    && chmod +x ${SUPERSET_HOME}/*.sh

USER superset

RUN pip install \
    --quiet \
    --no-cache-dir \
        pyhive[hive]==0.6.4 \
        acryl-datahub[superset]

ENTRYPOINT [ "/bin/bash", "-c" ]
CMD [ "${SUPERSET_HOME}/start-superset.sh" ]
