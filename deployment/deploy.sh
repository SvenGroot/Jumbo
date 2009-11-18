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

deployConfig()
{
    local configFile=$1
    local configBaseName=$2
    local slave=$3
    if [ -f $configFile ]; then
	scp $configFile $slave:$jumboDir/$configBaseName.config > /dev/null &
    fi
}

scriptDir=$(dirname $0)
. $scriptDir/jumbo-config.sh

for group in `cat $scriptDir/groups`; do
    groupDfsConfig=$(checkConfig dfs $group)
    groupJetConfig=$(checkConfig jet $group)

    for slave in `cat $scriptDir/$group`; do
	echo $group/$slave: creating directory
	ssh $slave mkdir -p $jumboDir &
    done
    wait

    for slave in `cat $scriptDir/$group`; do
	echo $group/$slave: deploying binaries
	scp -r $scriptDir/../nantbin/* $slave:$jumboDir > /dev/null &
    done
    wait

    for slave in `cat $scriptDir/$group`; do
	echo $group/$slave: deploying config
	scp $scriptDir/jumbo-config.sh $slave:$jumboDir > /dev/null &
	deployConfig $groupDfsConfig dfs $slave
	deployConfig $groupJetConfig jet $slave
    done
    wait
done
