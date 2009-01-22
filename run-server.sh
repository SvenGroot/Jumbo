#!/bin/sh
scriptDir=$(dirname $0)
. $scriptDir/jumbo-config.sh
startStop=$1
command=$2

pid=$pidDir/$command.pid

case $startStop in
    (start)
    mkdir -p "$pidDir"
    if [ -f $pid ]; then
		if kill -0 `cat $pid` > /dev/null 2>&1; then
			echo $command running as process `cat $pid`. Stop it first.
			exit 1
		fi
    fi

    echo starting $command
    cd $scriptDir
    if [ $command = "DfsWeb" ]; then
        cd DfsWeb
		nohup xsp2 --port $dfsWebPort --nonstop --pidfile $pid > /dev/null 2>&1 < /dev/null &
		cd ..
    elif [ $command = "JetWeb" ]; then
		cd JetWeb
		nohup xsp2 --port $jetWebPort --nonstop --pidfile $pid > /dev/null 2>&1 < /dev/null &
		cd ..
    else
		nohup mono $command.exe > /dev/null 2>&1 < /dev/null &
		echo $! > $pid
    fi
    sleep 1;
    ;;
    (stop)
    if [ -f $pid ]; then
		if kill -0 `cat $pid` > /dev/null 2>&1; then
			echo stopping $command
			kill `cat $pid`
		else
			echo no $command to stop
		fi
    else
		echo no $command to stop
    fi
    ;;
esac
