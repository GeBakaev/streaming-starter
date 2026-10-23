# Official Flink image (bundles a matching Java + Flink distribution).
FROM --platform=linux/amd64 flink:1.18.1-scala_2.12-java11

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && pip3 install --no-cache-dir apache-flink==1.18.1 kafka-python \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# v2: Kafka connector for Flink 1.18
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.18/flink-sql-connector-kafka-3.2.0-1.18.jar /opt/flink/lib/
RUN chmod 644 /opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.18.jar

# The job lives in the image so we can submit it from the JobManager.
COPY job.py /opt/job/job.py
