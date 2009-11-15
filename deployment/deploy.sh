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

    echo $configName;
}

deployConfig()
{
    local configFile=$1
    local slave=$2
    if [ -f $configFile ]; then
	scp $configFile $slave:$jumboDir/dfs.config > /dev/null
    fi
}

scriptDir=$(dirname $0)
. $scriptDir/jumbo-config.sh

for group in `cat $scriptDir/groups`; do
    groupDfsConfig=$(checkConfig dfs $group)
    groupJetConfig=$(checkConfig jet $group)

    for slave in `cat $scriptDir/$group`; do
	echo $group/$slave: deploying
	ssh $slave mkdir -p $jumboDir
	scp -r $scriptDir/../nantbin/* $slave:$jumboDir > /dev/null
	deployConfig $groupDfsConfig $slave
	deployConfig $groupJetConfig $slave
    done
done
