#!/bin/bash
scriptDir=$(dirname $0)
. $scriptDir/jumbo-config.sh
startStop=$1

if [ $startStop = "start" ]; then
	$scriptDir/run-server.sh $startStop NameServer
fi
for slave in `cat $scriptDir/slaves`; do
    echo $slave
    ssh $slave $jumboDir/run-server.sh $startStop DataServer
done
if [ $startStop = "stop" ]; then
	$scriptDir/run-server.sh $startStop NameServer
fi
