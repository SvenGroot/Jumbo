# This file provides configuration for the various PowerShell scripts used to interact with Jumbo.
# On Linux, please use the shell scripts (*.sh) instead.
# For Jumbo's main configuration, see bin/common.config, bin/dfs.config, and bin/jet.config

# Set JUMBO_HOME to the location of Jumbo's main directory (the directory containing the scripts, not the bin directory)
$JUMBO_HOME="D:\jumbodist"

# Directory where the process IDs of the various Jumbo processes are stored for use by the run-dfs.ps1 and run-jet.ps1 scripts
$JUMBO_PID=[System.IO.Path]::GetTempPath()

# This controls the location of the "out" and "err" log files. In order for DfsWeb/JetWeb to be able to retrieve these files correctly, this
# value should be said to the same value as the log directory in bin/common.config
$JUMBO_LOG="$JUMBO_HOME\bin\log"

# Ports for the admin web sites
$JUMBO_DFSWEB_PORT=35000
$JUMBO_JETWEB_PORT=36000

# Master node names for the run-dfs.ps1 and run-jet.ps1 scripts. Leaving this at localhost is okay as long as you only ever invoke those scripts from the respective servers.
$JUMBO_NAMESERVER="localhost"
$JUMBO_JOBSERVER="localhost"

# Set this to the path of IIS Express, or set to $null is not using IIS Express for the admin sites
$IISEXPRESS=[System.IO.Path]::Combine([Environment]::GetFolderPath("ProgramFiles"), "IIS Express", "iisexpress.exe")
