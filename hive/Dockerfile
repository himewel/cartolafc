FROM alpine:3.14

ENV JAVA_HOME "/usr/lib/jvm/default-jvm"
ENV HADOOP_HOME "/opt/hadoop"
ENV HIVE_HOME "/opt/hive"

ENV HADOOP_HEAPSIZE 2048
ENV LD_LIBRARY_PATH "${HADOOP_HOME}/lib/native/:${LD_LIBRARY_PATH}"
ENV HADOOP_OPTS "-Djava.library.path=${HADOOP_HOME}/lib"
ENV HADOOP_OPTS "${HADOOP_OPTS} -server -verbose:gc -XX:+PrintGCDetails"
ENV HADOOP_OPTS "${HADOOP_OPTS} -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps "
ENV HADOOP_OPTS "${HADOOP_OPTS} -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/var/log/hive"

ENV HADOOP_CONF_DIR "/etc/hive"
ENV HIVE_CONF_DIR "/etc/hive"

ENV PATH "${PATH}:${JAVA_HOME}/bin"
ENV PATH "${PATH}:${HIVE_HOME}/bin"
ENV PATH "${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin"

RUN apk add --no-cache \
        bash \
        gcompat \
        openjdk8

RUN wget -q https://ftp.unicamp.br/pub/apache/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz \
    && tar -xf hadoop-3.2.2.tar.gz \
    && rm -rf hadoop-3.2.2.tar.gz \
    && mv hadoop-3.2.2 ${HADOOP_HOME}

RUN wget -q https://ftp.unicamp.br/pub/apache/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz \
    && tar -xf apache-hive-3.1.2-bin.tar.gz \
    && rm -rf apache-hive-3.1.2-bin.tar.gz \
    && mv apache-hive-3.1.2-bin ${HIVE_HOME} \
    && rm ${HIVE_HOME}/lib/guava-19.0.jar \
    && cp ${HADOOP_HOME}/share/hadoop/hdfs/lib/guava-27.0-jre.jar ${HIVE_HOME}/lib/

WORKDIR ${HIVE_HOME}/bin

COPY ./start-metastore.sh .

RUN chmod +x ./start-metastore.sh

ENTRYPOINT [ "bash" ]
