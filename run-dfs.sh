#!/bin/sh
scriptDir=$(dirname $0)
. $scriptDir/jumbo-config.sh
startStop=$1

if [ "$startStop" != "start" -a "$startStop" != "stop" ]; then
    echo "Usage: run-dfs.sh (start|stop)"
fi

if [ "$startStop" = "start" ]; then
    ssh $JUMBO_NAMESERVER $JUMBO_HOME/run-server.sh $startStop NameServer 2>&1 | sed "s/^/$JUMBO_NAMESERVER: /"
    ssh $JUMBO_NAMESERVER $JUMBO_HOME/run-server.sh $startStop DfsWeb 2>&1 | sed "s/^/$JUMBO_NAMESERVER: /"
    sleep 1
fi

for group in $(cat $scriptDir/groups); do
    if [ "$group" != "masters" ]; then
        for slave in $(cat $scriptDir/$group); do
            ssh $slave $JUMBO_HOME/run-server.sh $startStop DataServer 2>&1 | sed "s/^/$slave: /" &
        done
    fi
done
wait

if [ "$startStop" = "stop" ]; then
    sleep 1
    ssh $JUMBO_NAMESERVER $JUMBO_HOME/run-server.sh $startStop DfsWeb 2>&1 | sed "s/^/$JUMBO_NAMESERVER: /"
    ssh $JUMBO_NAMESERVER $JUMBO_HOME/run-server.sh $startStop NameServer 2>&1 | sed "s/^/$JUMBO_NAMESERVER: /"
fi
