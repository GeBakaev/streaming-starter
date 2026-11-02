# Official Flink image (bundles a matching Java + Flink distribution).
# Flink 2.1 rather than 2.3: as of Oct 2026, 2.1 is the newest version with
# released Kafka AND Iceberg connectors.
FROM --platform=linux/amd64 flink:2.1.3-scala_2.12-java21

# PyFlink must match the Flink version exactly.
# PIP_BREAK_SYSTEM_PACKAGES: newer Ubuntu images block pip installs into the system
# Python (PEP 668). Fine inside a single-purpose container.
ENV PIP_BREAK_SYSTEM_PACKAGES=1
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && pip3 install --no-cache-dir apache-flink==2.1.3 kafka-python \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# v2: Kafka connector (5.0.0 is the release built for Flink 2.1)
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.1/flink-sql-connector-kafka-5.0.0-2.1.jar /opt/flink/lib/

# v3: Iceberg sink.
#  - iceberg-flink-runtime: the Flink <-> Iceberg connector (Iceberg 1.11.0, built for Flink 2.1)
#  - iceberg-aws-bundle:    S3FileIO, so Iceberg can read/write S3-compatible storage (SeaweedFS locally)
#  - hadoop-client-api/runtime: Hadoop classes the Iceberg connector still needs at runtime
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.1/1.11.0/iceberg-flink-runtime-2.1-1.11.0.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar /opt/flink/lib/
RUN chmod 644 /opt/flink/lib/*.jar

# The job lives in the image so we can submit it from the JobManager.
COPY job.py /opt/job/job.py
