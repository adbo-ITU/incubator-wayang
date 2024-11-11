FROM mcr.microsoft.com/devcontainers/java:1-11-bookworm

WORKDIR /opt/spark

RUN curl -O https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz
RUN tar -xvf spark-3.2.0-bin-hadoop3.2.tgz
RUN chmod -R 777 /opt/spark

WORKDIR /opt/hadoop
RUN curl -O https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0-aarch64.tar.gz
RUN tar -xvf hadoop-3.4.0-aarch64.tar.gz
RUN chmod -R 777 /opt/hadoop

WORKDIR /opt/coursier
RUN curl -fL https://github.com/VirtusLab/coursier-m1/releases/latest/download/cs-aarch64-pc-linux.gz --output cs.gz 
RUN cat cs.gz | gzip -d > cs && chmod +x cs

ENV PATH=$PATH:/home/vscode/.local/share/coursier/bin

# Spark environment variables
ENV SPARK_HOME=/opt/spark/spark-3.2.0-bin-hadoop3.2
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Hadoop environment variables
ENV HADOOP_HOME=/opt/hadoop/hadoop-3.4.0
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Wayang environment variables
ENV WAYANG_HOME=/workspaces/incubator-wayang/wayang-0.7.1
ENV PATH=$PATH:$WAYANG_HOME/bin