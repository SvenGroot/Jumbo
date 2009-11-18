#!/bin/bash
scriptDir=$(dirname $0)
. $scriptDir/jumbo-config.sh
startStop=$1

if [ $startStop = "start" ]; then
    ssh $nameServer $jumboDir/run-server.sh $startStop NameServer 2>&1 | sed "s/^/$nameServer: /"
    ssh $nameServer $jumboDir/run-server.sh $startStop DfsWeb 2>&1 | sed "s/^/$nameServer: /"
    sleep 1
fi

for group in `cat $scriptDir/groups`; do
    if [ "$group" != "masters" ]; then
	for slave in `cat $scriptDir/$group`; do
	    ssh $slave $jumboDir/run-server.sh $startStop DataServer 2>&1 | sed "s/^/$slave: /" &
	done
    fi
done

wait
if [ $startStop = "stop" ]; then
    sleep 1
    ssh $nameServer $jumboDir/run-server.sh $startStop DfsWeb | sed "s/^/$nameServer: /"
    ssh $nameServer $jumboDir/run-server.sh $startStop NameServer | sed "s/^/$nameServer: /"
fi
