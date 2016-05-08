#!/bin/bash
PYTHON_FILE=$1
OUTPUT=$2
LOCAL_OUTPUT=$3
NUM_EXECS=$4
NAME="HW6"

HD=hadoop
SP=spark-submit

$HD fs -rm -r -skipTrash $OUTPUT
$SP --name "$NAME" --num-executors $NUM_EXECS $PYTHON_FILE $OUTPUT
rm -f $LOCAL_OUTPUT
$HD fs -cat $OUTPUT/part* > $LOCAL_OUTPUT
