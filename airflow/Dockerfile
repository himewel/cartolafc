FROM apache/airflow:2.1.0

ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}/include"

COPY scripts ./scripts

RUN curl -O https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh \
    && chmod +x ./wait-for-it.sh

ENTRYPOINT [ "/bin/bash" ]
