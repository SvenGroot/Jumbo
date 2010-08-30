#!/bin/bash

checkConfig()
{
    local configName=$1
    local group=$2
    configFile=$scriptDir/$configName.$group.config
    if [ -f $configFile ]; then
	echo 1>&2 $group: using group config file $configName.$group.config
    else
	echo 1>&2 $group: no custom $configName.config for this group.
    fi

    echo $configFile;
}

scriptDir=$(dirname $0)
. $scriptDir/jumbo-config.sh
mode=$1
if [ "$mode" == "" ]; then
    mode="all"
fi

for group in `cat $scriptDir/groups`; do
    groupDfsConfig=$(checkConfig dfs $group)
    groupJetConfig=$(checkConfig jet $group)

    for slave in `cat $scriptDir/$group`; do
	echo $group/$slave: deploying $mode
	$scriptDir/server-deploy.sh $slave $groupDfsConfig $groupJetConfig $mode &
    done
    wait
done