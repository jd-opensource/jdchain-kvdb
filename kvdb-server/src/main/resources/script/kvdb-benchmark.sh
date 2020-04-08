#!/bin/bash

HOME=$(cd `dirname $0`;cd ../; pwd)
KVDB=$(ls $HOME/libs | grep kvdb-benchmark)
JVM_SET="-Xmx2g -Xms2g"
PROC_INFO=$HOME/libs/$KVDB
if [ ! -n "$KVDB" ]; then
  echo "Can not find kvdb-benchmark !!!"
else
  java -jar $JVM_SET $PROC_INFO $*
fi