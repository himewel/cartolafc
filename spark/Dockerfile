FROM python:3.8-buster

ENV JAVA_HOME "/usr/lib/jvm/java-11-openjdk-amd64"
ENV SPARK_HOME "/opt/spark"

ENV PATH "${PATH}:${JAVA_HOME}/bin"
ENV PATH "${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin"
ENV PYTHONPATH "${PYTHONPATH}:${SPARK_HOME}/python:${SPARK_HOME}/python/lib"

ENV SPARK_NO_DAEMONIZE TRUE
ENV SPARK_CONF_DIR "/etc/spark"

RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install --no-install-recommends --yes \
       default-jdk \
    && apt-get remove --purge --yes ${builds_deps} \
    && apt-get clean \
    && rm -rf -- /var/lib/apt/lists/*

RUN curl --continue-at - "https://ftp.unicamp.br/pub/apache/spark/spark-3.0.2/spark-3.0.2-bin-hadoop3.2.tgz" \
        -o ./spark-3.0.2-bin-hadoop3.2.tgz
RUN tar -xf spark-3.0.2-bin-hadoop3.2.tgz \
    && rm -rf spark-3.0.2-bin-hadoop3.2.tgz \
    && mv spark-3.0.2-bin-hadoop3.2 ${SPARK_HOME}

RUN useradd --create-home --shell /bin/bash spark

USER spark

COPY ./requirements.txt .

RUN pip3 install -r requirements.txt
