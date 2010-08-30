#!/bin/bash

deployConfig()
{
    local configFile=$1
    local configBaseName=$2
    if [ -f $configFile ]; then
        scp $configFile $slave:$jumboDir/$configBaseName.config > /dev/null
    else
	scp $scriptDir/../nantbin/$configBaseName.config $slave:$jumboDir/$configBaseName.config > /dev/null
    fi
}

scriptDir=$(dirname $0)
. $scriptDir/jumbo-config.sh
slave=$1
dfsConfigFile=$2
jetConfigFile=$3
mode=$4

if [ "$mode" == "all" ]; then
    ssh $slave mkdir -p $jumboDir
    scp -r $scriptDir/../nantbin/* $slave:$jumboDir > /dev/null
fi

if [ "$mode" == "all" ] || [ "$mode" == "config" ]; then
    scp $scriptDir/jumbo-config.sh $slave:$jumboDir > /dev/null
    deployConfig $dfsConfigFile dfs
    deployConfig $jetConfigFile jet
fi
