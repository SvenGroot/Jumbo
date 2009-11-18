#!/bin/bash
scriptDir=$(dirname $0)
. $scriptDir/jumbo-config.sh
startStop=$1

if [ $startStop = "start" ]; then
    ssh $jobServer $jumboDir/run-server.sh $startStop JobServer 2>&1 | sed "s/^/$jobServer: /"
    ssh $jobServer $jumboDir/run-server.sh $startStop JetWeb 2>&1 | sed "s/^/$jobServer: /"
    sleep 1
fi

for group in `cat $scriptDir/groups`; do
    if [ "$group" != "masters" ]; then
        for slave in `cat $scriptDir/$group`; do
	    ssh $slave $jumboDir/run-server.sh $startStop TaskServer 2>&1 | sed "s/^/$slave: /" &
        done
    fi
done
wait

if [ $startStop = "stop" ]; then
    sleep 1
    ssh $jobServer $jumboDir/run-server.sh $startStop JetWeb 2>&1 | sed "s/^/$jobServer: /"
    ssh $jobServer $jumboDir/run-server.sh $startStop JobServer 2>&1 | sed "s/^/$jobServer: /"
fi
