#!/bin/bash
JAVA_ARGS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=150 -XX:+ParallelRefProcEnabled -Xmx14617M"


for FT in csv json
do
  OFILE="result.${FT}"
  rm ${OFILE}
  java ${JAVA_ARGS} -jar target/benchmarks.jar -rf ${FT} -rff ${OFILE} "$@"
done