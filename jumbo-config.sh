#!/bin/sh

# This file provides configuration for the various scripts used to interact with Jumbo.
# For Jumbo's main configuration, see bin/common.config, bin/dfs.config, and bin/jet.config

# Set JUMBO_HOME to the location of the "build" directory (the directory containing the scripts, not the bin directory)
JUMBO_HOME=

# Directory where the process IDs of the various Jumbo processes are stored for use by the run-dfs.sh and run-jet.sh scripts
JUMBO_PID=/tmp

# This controls the location of the "out" and "err" log files. In order for DfsWeb/JetWeb to be able to retrieve these files correctly, this
# value should be said to the same value as the log directory in bin/common.config
JUMBO_LOG=$JUMBO_HOME/bin/log

# Ports for the admin web sites
JUMBO_DFSWEB_PORT=35000
JUMBO_JETWEB_PORT=36000

# Master node names for the run-dfs.sh and run-jet.sh scripts. Leaving this at localhost is okay as long as you only ever invoke those scripts from the respective servers.
JUMBO_NAMESERVER=localhost
JUMBO_JOBSERVER=localhost

# Customize this to use a different mono executable or change the invocation
MONO="mono"
XSP="xsp4"
export MONO_ENV_OPTIONS="--gc=sgen"

# If you are using Mono shared memory (export MONO_ENABLED_SHM=1), make sure you also set MONO_SHARED_DIR to a location that is *not* an NFS path
# Shared memory is disabled by default in Mono 2.8 and 3.0; best to leave it that way (Jumbo doesn't need it).
#export MONO_SHARED_DIR=/tmp
