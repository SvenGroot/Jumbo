param(
    [parameter(Mandatory=$true, Position=0)][string]$StartStop
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. $scriptDir\Jumbo-Config.ps1

function Invoke-Server($HostName, [string]$Command)
{
    # Invoke-Command requires WinRM. Because WinRM is not enabled by default on client versions of Windows
    # and only works with private networks, I special case localhost so these scripts can still be used for testing on those systems.
    if( $HostName -eq "localhost" )
    {
        &"$scriptDir\Run-Server.ps1" $StartStop $Command |% { "${HostName}: $_" }
    }
    else
    {
        $job = Invoke-Command -ComputerName $HostName -ScriptBlock { &"$Using:scriptDir\Run-Server.ps1" $Using:StartStop $Using:Command } -AsJob
        $job | Wait-Job | Receive-Job |% { "$($_.PSComputerName): $_" }
    }
}

if( $StartStop -ne "start" -and $StartStop -ne "stop" )
    { throw "Must specify start or stop" }

if( $StartStop -eq "start" )
{
    Invoke-Server $JUMBO_NAMESERVER NameServer
    Invoke-Server $JUMBO_NAMESERVER DfsWeb
}

$HostNames = Get-Content groups |? { $_ -ne "masters" } |% { Get-Content $_ }
Invoke-Server $HostNames DataServer

if( $StartStop -eq "stop" )
{
    Invoke-Server $JUMBO_NAMESERVER NameServer
    Invoke-Server $JUMBO_NAMESERVER DfsWeb
}