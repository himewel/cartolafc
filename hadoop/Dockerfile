FROM alpine:3.14

ENV JAVA_HOME "/usr/lib/jvm/default-jvm"
ENV HADOOP_HOME "/opt/hadoop"
ENV HADOOP_CONF_DIR "/etc/hadoop"
ENV LD_LIBRARY_PATH "${HADOOP_HOME}/lib/native/:${LD_LIBRARY_PATH}"
ENV HADOOP_OPTS "-Djava.library.path=${HADOOP_HOME}/lib"

ENV PATH "${PATH}:${JAVA_HOME}/bin"
ENV PATH "${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin"

RUN apk add --no-cache \
        bash \
        gcompat \
        openjdk11 \
    && wget -q https://ftp.unicamp.br/pub/apache/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz \
    && tar -xf hadoop-3.2.2.tar.gz \
    && rm -rf hadoop-3.2.2.tar.gz \
    && mv hadoop-3.2.2 ${HADOOP_HOME} \
    && adduser hadoop --disabled-password \
    && mkdir --parents /hadoop/dfs/data \
    && mkdir --parents /hadoop/dfs/name \
    && chown --recursive hadoop /hadoop

WORKDIR ${HADOOP_HOME}

USER hadoop

COPY start.sh .

ENTRYPOINT [ "bash" ]
CMD ["./start.sh"]
