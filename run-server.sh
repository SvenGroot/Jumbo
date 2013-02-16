#!/bin/sh
# You should not run this script directly; use run-dfs.sh and run-jet.sh instead
scriptDir=$(dirname $0)
. $scriptDir/jumbo-config.sh
startStop=$1
command=$2

pid=$JUMBO_PID/jumbo-$command-$(hostname).pid

case $startStop in
    (start)
        mkdir -p "$JUMBO_PID"
        if [ -f $pid ]; then
            if kill -0 $(cat $pid) > /dev/null 2>&1; then
                echo "$command running as process $(cat $pid). Stop it first."
                exit 1
            fi
        fi

        echo starting $command
        if [ "$command" = "DfsWeb" ]; then
            cd $JUMBO_HOME/bin/DfsWeb
            nohup $XSP --port $JUMBO_DFSWEB_PORT --nonstop --pidfile $pid > $JUMBO_LOG/out-$command-$(hostname).txt 2> $JUMBO_LOG/err-$command-$(hostname).txt < /dev/null &
        elif [ "$command" = "JetWeb" ]; then
            cd $JUMBO_HOME/bin/JetWeb
            nohup $XSP --port $JUMBO_JETWEB_PORT --nonstop --pidfile $pid > $JUMBO_LOG/out-$command-$(hostname).txt 2> $JUMBO_LOG/err-$command-$(hostname).txt < /dev/null &
        else
            cd $JUMBO_HOME/bin
            nohup $MONO $command.exe > $JUMBO_LOG/out-$command-$(hostname).txt 2> $JUMBO_LOG/err-$command-$(hostname).txt < /dev/null &
            echo $! > $pid
        fi
        ;;
    (stop)
        if [ -f $pid ]; then
            if kill -0 $(cat $pid) > /dev/null 2>&1; then
                echo stopping $command
                kill $(cat $pid)
            else
                echo no $command to stop
            fi
        else
            echo no $command to stop
        fi
        ;;
esac
