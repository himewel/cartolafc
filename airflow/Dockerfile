FROM apache/airflow:2.1.0

ARG USER_ID=1000
ARG GROUP_ID=1000

ENV JAVA_HOME "/usr/lib/jvm/java-8-openjdk-amd64"
ENV HADOOP_HOME "/opt/hadoop"
ENV HIVE_HOME "/opt/hive"

ENV HADOOP_CONF_DIR "/etc/hadoop"
ENV HIVE_CONF_DIR "/etc/hadoop"

ENV PATH "${PATH}:${JAVA_HOME}/bin"
ENV PATH "${PATH}:${HIVE_HOME}/bin"
ENV PATH "${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin"
ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}/include"

USER root

RUN mkdir -p /usr/share/man/man1 \
    && (echo "deb http://security.debian.org/debian-security stretch/updates main" >> /etc/apt/sources.list) \
    && apt-get update \
    && apt-get install --no-install-recommends --yes \
        openjdk-8-jdk \
        libsasl2-dev \
        gcc \
        g++ \
    && apt-get clean \
    && rm -rf -- /var/lib/apt/lists/*

RUN curl -O https://ftp.unicamp.br/pub/apache/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz \
    && tar -xf hadoop-3.2.2.tar.gz \
    && rm -rf hadoop-3.2.2.tar.gz \
    && mv hadoop-3.2.2 ${HADOOP_HOME}

RUN curl -O https://ftp.unicamp.br/pub/apache/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz \
    && tar -xf apache-hive-3.1.2-bin.tar.gz \
    && rm -rf apache-hive-3.1.2-bin.tar.gz \
    && mv apache-hive-3.1.2-bin ${HIVE_HOME} \
    && rm ${HIVE_HOME}/lib/guava-19.0.jar \
    && cp ${HADOOP_HOME}/share/hadoop/hdfs/lib/guava-27.0-jre.jar ${HIVE_HOME}/lib/

COPY scripts ./scripts

RUN usermod --uid ${USER_ID} airflow \
    && groupmod --gid ${GROUP_ID} airflow \
    && chown --recursive airflow ${AIRFLOW_HOME} \
    && chmod +x ./scripts/*.sh

USER airflow

COPY ./airflow.cfg ./airflow.cfg
COPY ./requirements.txt ./requirements.txt
RUN pip install --no-cache-dir --user --requirement ./requirements.txt

ENTRYPOINT [ "/bin/bash" ]
