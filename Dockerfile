FROM apache/spark
USER root
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install minio
# COPY ./jars/*.jar /opt/spark/jars/
USER spark