#!/bin/sh

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
    if [ -f $configFile ]; then
        scp $configFile $slave:$JUMBO_HOME/$configBaseName.config > /dev/null
    else
        scp $scriptDir/bin/$configBaseName.config $slave:$JUMBO_HOME/bin/$configBaseName.config > /dev/null
    fi
}


scriptDir=$(dirname $0)
. $scriptDir/jumbo-config.sh
mode=$1
if [ "$mode" = "" ]; then
    mode="all"
fi

for group in $(cat $scriptDir/groups); do
    echo "Deploying group '$group'..."
    groupCommonConfig=$(checkConfig common $group)
    groupDfsConfig=$(checkConfig dfs $group)
    groupJetConfig=$(checkConfig jet $group)

    for slave in $(cat $scriptDir/$group); do
        echo $group/$slave: deploying $mode
        {
            if [ "$mode" = "all" ]; then
                ssh $slave mkdir -p $JUMBO_HOME
                scp -r $scriptDir/* $slave:$JUMBO_HOME > /dev/null
            fi

            if [ "$mode" = "all" -o "$mode" = "config" ]; then
                scp $scriptDir/jumbo-config.sh $slave:$JUMBO_HOME > /dev/null
                deployConfig $groupCommonConfig common
                deployConfig $groupDfsConfig dfs
                deployConfig $groupJetConfig jet
            fi
        } 2>&1 | sed "s/^/$group\/$slave: /" &
    done
    wait
done
