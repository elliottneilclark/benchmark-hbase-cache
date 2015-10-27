#!/bin/bash

set -e
set -x

JAVA_ARGS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=150 -XX:+ParallelRefProcEnabled -Xmx14617M"

mvn clean
mvn clean package
clear
mvn -version

FT="csv"
OFILE="result.${FT}"
[[ -f "$OFILE" ]] &&  rm ${OFILE}
java ${JAVA_ARGS} -jar target/benchmarks.jar -rf ${FT} -rff ${OFILE} "$@"

