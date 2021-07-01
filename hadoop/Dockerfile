FROM debian:buster-slim

ENV JAVA_HOME "/usr/lib/jvm/java-11-openjdk-amd64"
ENV HADOOP_HOME "/opt/hadoop"
ENV HADOOP_CONF_DIR "/etc/hadoop"

ENV PATH "${PATH}:${JAVA_HOME}/bin"
ENV PATH "${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin"

RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install --no-install-recommends --yes \
       default-jdk \
       curl \
    && apt-get remove --purge --yes ${builds_deps} \
    && apt-get clean \
    && rm -rf -- /var/lib/apt/lists/*

RUN curl --continue-at - "https://ftp.unicamp.br/pub/apache/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz" \
        -o ./hadoop-3.2.2.tar.gz \
    && tar -xf hadoop-3.2.2.tar.gz \
    && rm -rf hadoop-3.2.2.tar.gz \
    && mv hadoop-3.2.2 ${HADOOP_HOME}

WORKDIR ${HADOOP_HOME}

RUN useradd --create-home --shell /bin/bash hadoop \
    && mkdir --parents /hadoop/dfs/data \
    && mkdir --parents /hadoop/dfs/name \
    && chown -R hadoop /hadoop

USER hadoop

COPY start.sh .

CMD ["bash", "./start.sh"]
