#!/bin/bash
scriptDir=$(dirname $0)
. $scriptDir/jumbo-config.sh
startStop=$1

if [ $startStop = "start" ]; then
    $scriptDir/run-server.sh $startStop NameServer
    $scriptDir/run-server.sh $startStop DfsWeb
    sleep 1
fi
for slave in `cat $scriptDir/slaves`; do
    ssh $slave $jumboDir/run-server.sh $startStop DataServer 2>&1 | sed "s/^/$slave: /" &
done
wait
if [ $startStop = "stop" ]; then
    sleep 1
    $scriptDir/run-server.sh $startStop DfsWeb
    $scriptDir/run-server.sh $startStop NameServer
fi
