FROM python:3.11-bullseye
RUN apt-get update
RUN apt-get install -y --no-install-recommends openjdk-17-jdk
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
ENV SPARK_HOME="/home/sparkuser/spark"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-arm64"
ENV PATH="${JAVA_HOME}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN mkdir -p ${SPARK_HOME}

WORKDIR ${SPARK_HOME}

RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.7/spark-3.5.7-bin-hadoop3.tgz
RUN tar xvzf spark-3.5.7-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1 \
    && rm -rf spark-3.5.7-bin-hadoop3.tgz
ENV SPARK_MASTER_PORT="7077"
ENV SPARK_MASTER_HOST="spark-master"

RUN curl -fL https://github.com/coursier/coursier/releases/download/v2.1.25-M23/cs-aarch64-pc-linux.gz | gzip -d > cs && chmod +x cs
ENV PATH="$PATH:/root/.local/share/coursier/bin"
RUN ./cs setup

RUN wget -P ${SPARK_HOME}/jars/ https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

RUN useradd -u 1000 -m -d /home/sparkuser sparkuser
ENV HOME="/home/sparkuser"
RUN chown -R 1000:1000 ${SPARK_HOME}
USER root

COPY ./spark-defaults.conf "${SPARK_HOME}/conf"

ENTRYPOINT ["/bin/bash"]