#!/bin/bash
scriptDir=$(dirname $0)
. $scriptDir/jumbo-config.sh

for group in `cat $scriptDir/groups`; do
    groupDfsConfig=$scriptDir/dfs.$group.config
    if [ -f $groupDfsConfig ]; then
	echo $group: using group config file dfs.$group.config
    else
	echo $group: no custom dfs.config for this group.
    fi

    groupJetConfig=$scriptDir/jet.$group.config
    if [ -f $groupJetConfig ]; then
	echo $group: using group config file jet.$group.config
    else
	echo $group: no custom jet.config for this group.
    fi

    for slave in `cat $scriptDir/$group`; do
	echo $group/$slave: deploying
	ssh $slave mkdir -p $jumboDir
	scp -r $scriptDir/../nantbin/* $slave:$jumboDir > /dev/null
	if [ -f $groupDfsConfig ]; then
	    scp $groupDfsConfig $slave:$jumboDir/dfs.config > /dev/null
	fi
	if [ -f $groupJetConfig ]; then
	    scp $groupJetConfig $slave:$jumboDir/jet.config > /dev/null
	fi
    done
done
