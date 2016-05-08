#!/bin/bash
MAPPER=$1
REDUCER=$2
MAPPER_NAME=$(basename $MAPPER)
REDUCER_NAME=$(basename $REDUCER)
INPUT=$3
OUTPUT=$4
NUM_MAPPERS=$5
NUM_REDUCERS=$6
LOCAL_OUTPUT=$7
STREAMING_JAR=/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar
hadoop fs -rm -r -skipTrash "${OUTPUT}"
hadoop jar ${STREAMING_JAR} \
    -D mapred.job.name='BDM_HW5' \
    -D mapred.map.tasks=${NUM_MAPPERS} \
    -files "${MAPPER}","${REDUCER}" \
    -mapper "${MAPPER_NAME}" \
    -reducer "${REDUCER_NAME}" \
    -input "${INPUT}" \
    -output "${OUTPUT}" \
    -numReduceTasks ${NUM_REDUCERS}

if [ ! -z "${LOCAL_OUTPUT}" ]; then
    rm -f "${LOCAL_OUTPUT}"
    hadoop fs -getmerge "${OUTPUT}/part*" "${LOCAL_OUTPUT}"
fi
