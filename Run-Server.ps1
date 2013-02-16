param(
    [parameter(Mandatory=$true, Position=0)][string]$StartStop,
    [parameter(Mandatory=$true, Position=1)][string]$Command
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. $scriptDir\Jumbo-Config.ps1

$hostname = [System.Net.Dns]::GetHostName()
$pidFile = "$JUMBO_PID\jumbo-$Command-$hostname.pid"

switch( $StartStop )
{
    start
    {
        if( Test-Path $pidFile )
        {
            $jumboPid = Get-Content $pidFile
            if( Get-Process -Id $jumboPid -ErrorAction SilentlyContinue )
            {
                "$Command running as process $jumboPid. Stop it first."
                return
            }
        }

        "Starting $Command"
        if( $Command -eq "DfsWeb" )
        {
            if( $IISEXPRESS -ne $null -and (Test-Path $IISEXPRESS) )
            {
                $commandFile = $IISEXPRESS
                $commandArguments = "/path:$JUMBO_HOME\bin\DfsWeb /port:$JUMBO_DFSWEB_PORT"
            }
            else
            {
                "Can't find IIS express; not starting DfsWeb"
                return
            }
        }
        elseif( $Command -eq "JetWeb" )
        {
            if( $IISEXPRESS -ne $null -and (Test-Path $IISEXPRESS) )
            {
                $commandFile = $IISEXPRESS
                $commandArguments = "/path:$JUMBO_HOME\bin\JetWeb /port:$JUMBO_JETWEB_PORT"
            }
            else
            {
                "Can't find IIS express; not starting JetWeb"
                return
            }
        }
        else
        {
            $commandFile = "$JUMBO_HOME\bin\$Command.exe"
            $commandArguments = "fake" # Start-Process won't accept empty arguments, and all Jumbo servers ignore arguments anyway
        }

        $process = Start-Process -FilePath $commandFile -ArgumentList $commandArguments -NoNewWindow -PassThru -RedirectStandardOutput "$JUMBO_LOG\out-$Command-$hostname.log" -RedirectStandardError "$JUMBO_LOG\err-$Command-$hostname.log"
        Set-Content $pidFile $process.Id
    }
    stop
    {
        if( Test-Path $pidFile )
        {
            $jumboPid = Get-Content $pidFile
            if( Get-Process -Id $jumboPid -ErrorAction SilentlyContinue )
            {
                "Stopping $Command"
                Stop-Process -Id $jumboPid
                Remove-Item $pidFile
            }
            else
            {
                "No $Command to stop."
            }
        }
        else
        {
            "No $Command to stop."
        }
    }
}