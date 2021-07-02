FROM apache/airflow:2.1.0

ARG USER_ID=1000
ARG GROUP_ID=1000

ENV JAVA_HOME "/usr/lib/jvm/java-11-openjdk-amd64"
ENV HADOOP_HOME "/opt/hadoop"
ENV PATH "${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin"
ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}/include"

USER root

RUN apt-get update \
    && apt-get install default-jdk --no-install-recommends --yes \
    && apt-get clean \
    && rm -rf -- /var/lib/apt/lists/* \
    && curl --continue-at - "https://ftp.unicamp.br/pub/apache/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz" \
        -o ./hadoop-3.2.2.tar.gz \
    && tar -xf hadoop-3.2.2.tar.gz \
    && rm -rf hadoop-3.2.2.tar.gz \
    && mv hadoop-3.2.2 ${HADOOP_HOME}

COPY scripts ./scripts

RUN curl -O https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh \
    && chmod +x ./wait-for-it.sh \
    && usermod --uid ${USER_ID} airflow \
    && groupmod --gid ${GROUP_ID} airflow \
    && chown --recursive airflow ${AIRFLOW_HOME}

USER airflow

ENTRYPOINT [ "/bin/bash" ]
