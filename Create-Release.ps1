param(
    [parameter(Mandatory=$true, Position=0)][string]$TargetPath,
    [parameter(Mandatory=$false, Position=1)][string]$Configuration = "Debug"
)

$binItems = "Ookii.Jumbo\bin\$Configuration\Ookii.Jumbo.dll","Ookii.Jumbo\bin\$Configuration\Ookii.Jumbo.dll.config","Ookii.Jumbo\bin\$Configuration\Ookii.Jumbo.xml","Ookii.Jumbo\bin\$Configuration\log4net.dll","Ookii.Jumbo\bin\$Configuration\log4net.xml",
    "Ookii.Jumbo.Dfs\bin\$Configuration\Ookii.Jumbo.Dfs.dll","Ookii.Jumbo.Dfs\bin\$Configuration\Ookii.Jumbo.Dfs.xml",
    "Ookii.Jumbo.Jet\bin\$Configuration\Ookii.Jumbo.Jet.dll","Ookii.Jumbo.Jet\bin\$Configuration\Ookii.Jumbo.Jet.xml","Ookii.Jumbo.Jet\bin\$Configuration\Ookii.CommandLine.dll",
    "Ookii.Jumbo.Jet.Samples\bin\$Configuration\Ookii.Jumbo.Jet.Samples.dll","Ookii.Jumbo.Jet.Samples\bin\$Configuration\Ookii.Jumbo.Jet.Samples.xml",
    "x64\$Configuration\Ookii.Jumbo.Native.dll",
    "NameServer\bin\$Configuration\NameServer.exe","NameServer\bin\$Configuration\NameServer.exe.config",
    "DataServer\bin\$Configuration\DataServer.exe","DataServer\bin\$Configuration\DataServer.exe.config",
    "JobServer\bin\$Configuration\JobServer.exe","JobServer\bin\$Configuration\JobServer.exe.config",
    "TaskServer\bin\$Configuration\TaskServer.exe","TaskServer\bin\$Configuration\TaskServer.exe.config",
    "TaskHost\bin\$Configuration\TaskHost.exe","TaskHost\bin\$Configuration\TaskHost.exe.config",
    "DfsShell\bin\$Configuration\DfsShell.exe","DfsShell\bin\$Configuration\DfsShell.exe.config",
    "JetShell\bin\$Configuration\JetShell.exe","JetShell\bin\$Configuration\JetShell.exe.config",
    "DfsWeb","JetWeb",
    "common.config","dfs.config","jet.config"
$targetItems = "jumbo-config.sh","run-server.sh","run-dfs.sh","run-jet.sh","deploy.sh",
    "Jumbo-Config.ps1","Run-Server.ps1","Run-Dfs.ps1","Run-Jet.ps1",
    "groups","masters","slaves"
    
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
New-Item $TargetPath -ItemType Directory -Force | Out-Null
if( [System.IO.Directory]::GetFileSystemEntries($TargetPath).Length -gt 0 )
    { throw "Target directory not empty." }

$binPath = Join-Path $TargetPath bin

Write-Host "Copying distribution items"    
New-Item $binPath -ItemType Directory -Force | Out-Null
$binItems |% { Copy-Item (Join-Path $scriptPath $_) $binPath -Recurse }
$targetItems |% { Copy-Item (Join-Path $scriptPath $_) $TargetPath -Recurse }
Copy-Item (Join-Path $scriptPath "DfsShell-script") (Join-Path $TargetPath "DfsShell")
Copy-Item (Join-Path $scriptPath "JetShell-script") (Join-Path $TargetPath "JetShell")
