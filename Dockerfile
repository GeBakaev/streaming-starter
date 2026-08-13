# Official Flink image (bundles a matching Java + Flink distribution).
FROM --platform=linux/amd64 flink:1.18.1-scala_2.12-java11

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && pip3 install --no-cache-dir apache-flink==1.18.1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# The job lives in the image so we can submit it from the JobManager.
COPY job.py /opt/job/job.py
