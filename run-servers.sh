#!/bin/bash
scriptDir=$(dirname $0)
. $scriptDir/jumbo-config.sh
startStop=$1

$scriptDir/run-server.sh $startStop NameServer
for slave in `cat $scriptDir/slaves`; do
    echo $slave
    ssh $slave $jumboDir/run-server.sh $startStop DataServer
done
