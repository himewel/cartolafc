FROM apache/superset

ENV SUPERSET_HOME /opt/superset
ENV PYTHONPATH "${PYTHONPATH}:${SUPERSET_HOME}"
ENV SUPERSET_CONFIG_PATH "${SUPERSET_HOME}/superset_config.py"

WORKDIR ${SUPERSET_HOME}

USER root

COPY start-superset.sh .
COPY superset_config.py .

RUN chown -R superset ${SUPERSET_HOME} \
    && chmod +x ${SUPERSET_HOME}/start-superset.sh

USER superset

RUN pip install \
    --quiet \
    --no-cache-dir \
    pyhive[hive]==0.6.4

ENTRYPOINT [ "/bin/bash", "-c" ]
CMD [ "${SUPERSET_HOME}/start-superset.sh" ]
