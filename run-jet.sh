#!/bin/sh
scriptDir=$(dirname $0)
. $scriptDir/jumbo-config.sh
startStop=$1

if [ "$startStop" != "start" -a "$startStop" != "stop" ]; then
    echo "Usage: run-dfs.sh (start|stop)"
fi

if [ "$startStop" = "start" ]; then
    ssh $JUMBO_JOBSERVER $JUMBO_HOME/run-server.sh $startStop JobServer 2>&1 | sed "s/^/$JUMBO_JOBSERVER: /"
    ssh $JUMBO_JOBSERVER $JUMBO_HOME/run-server.sh $startStop JetWeb 2>&1 | sed "s/^/$JUMBO_JOBSERVER: /"
    sleep 1
fi

for group in $(cat $scriptDir/groups); do
    if [ "$group" != "masters" ]; then
        for slave in $(cat $scriptDir/$group); do
            ssh $slave $JUMBO_HOME/run-server.sh $startStop TaskServer 2>&1 | sed "s/^/$slave: /" &
        done
    fi
done
wait

if [ "$startStop" = "stop" ]; then
    sleep 1
    ssh $JUMBO_JOBSERVER $JUMBO_HOME/run-server.sh $startStop JetWeb 2>&1 | sed "s/^/$JUMBO_JOBSERVER: /"
    ssh $JUMBO_JOBSERVER $JUMBO_HOME/run-server.sh $startStop JobServer 2>&1 | sed "s/^/$JUMBO_JOBSERVER: /"
fi
