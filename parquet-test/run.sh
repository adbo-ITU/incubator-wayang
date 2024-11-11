#!/usr/bin/env bash

CLASS=$1

if [ -z "${CLASS}" ]; then
    echo "Target Class for execution was not provided"
    exit 1
fi

if [ -z "${WAYANG_HOME}" ]; then
  echo "The variable WAYANG_HOME it needs to be setup" >&2
  exit 1
fi

if [ -z "${SPARK_HOME}" ]; then
  echo "The variable SPARK_HOME it needs to be setup" >&2
  exit 1
fi

#if [ -z "${FLINK_HOME}" ]; then
#  echo "The variable FLINK_HOME it needs to be setup" >&2
#  exit 1
#fi

if [ -z "${HADOOP_HOME}" ]; then
  echo "The variable HADOOP_HOME it needs to be setup" >&2
  exit 1
fi

# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  RUNNER="${JAVA_HOME}/bin/java"
else
  if [ "$(command -v java)" ]; then
    RUNNER="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi

# Find Spark jars.
if [ -d "${SPARK_HOME}" ]; then
  SPARK_JARS_DIR="${SPARK_HOME}/jars"
fi

# Find Hadoop jars.
if [ -d "${HADOOP_HOME}" ]; then
  HADOOP_JARS_DIR="${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/common/lib/*"
fi

if [ "$(ls ${SPARK_JARS_DIR} | grep ^hadoop | wc -l)" == "0" ]; then

  HADOOP_JARS_DIR="${HADOOP_JARS_DIR}:${HADOOP_HOME}/share/hadoop/mapreduce/$(ls ${HADOOP_HOME}/share/hadoop/mapreduce | grep ^hadoop-mapreduce-client-common | grep -v tests | head -n 1)"
  HADOOP_JARS_DIR="${HADOOP_JARS_DIR}:${HADOOP_HOME}/share/hadoop/mapreduce/$(ls ${HADOOP_HOME}/share/hadoop/mapreduce | grep ^hadoop-mapreduce-client-core | grep -v tests | head -n 1)"
  HADOOP_JARS_DIR="${HADOOP_JARS_DIR}:${HADOOP_HOME}/share/hadoop/mapreduce/$(ls ${HADOOP_HOME}/share/hadoop/mapreduce | grep ^hadoop-mapreduce-client-jobclient | grep -v tests | head -n 1)"
  HADOOP_JARS_DIR="${HADOOP_JARS_DIR}:${HADOOP_HOME}/share/hadoop/hdfs/$(ls ${HADOOP_HOME}/share/hadoop/hdfs | grep ^hadoop-hdfs-client | grep -v tests | head -n 1)"
  HADOOP_JARS_DIR="${HADOOP_JARS_DIR}:${HADOOP_HOME}/share/hadoop/hdfs/lib/$(ls ${HADOOP_HOME}/share/hadoop/hdfs/lib | grep ^hadoop-annotations | grep -v tests | head -n 1)"
  HADOOP_JARS_DIR="${HADOOP_JARS_DIR}:${HADOOP_HOME}/share/hadoop/hdfs/lib/$(ls ${HADOOP_HOME}/share/hadoop/hdfs/lib | grep ^hadoop-auth | grep -v tests | head -n 1)"

fi


WAYANG_CODE="${WAYANG_HOME}/jars"

WAYANG_LIBS="${WAYANG_HOME}/libs"

WAYANG_CONF="${WAYANG_HOME}/conf"

# Bootstrap the classpath.
WAYANG_CLASSPATH="${WAYANG_CONF}/*:${WAYANG_CODE}/*:${WAYANG_LIBS}/*"
WAYANG_CLASSPATH="${SPARK_JARS_DIR}/*:${WAYANG_CLASSPATH}:${HADOOP_JARS_DIR}"

FLAGS=""
if [ "${FLAG_LOG}" = "true" ]; then
	FLAGS="${FLAGS} -Dlog4j.configuration=file://${WAYANG_CONF}/log4j.properties"
fi

if [ "${FLAG_WAYANG}" = "true" ]; then
	FLAGS="${FLAGS} -Dwayang.configuration=file://${WAYANG_CONF}/wayang.properties"
fi

if [ -n "${OTHER_FLAGS}" ]; then
	FLAGS="${FLAGS} ${OTHER_FLAGS}"
fi

# Wrap args in quotes to be able to execute args with parenthesis, spaces, etc
ARGS=""
for arg in $(echo ${@:2})
do
  ARGS="$ARGS \"${arg}\""
done

eval "$RUNNER $FLAGS -cp "${WAYANG_CLASSPATH}" $CLASS ${ARGS}"

