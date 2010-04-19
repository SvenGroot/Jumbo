$scriptPath = Split-Path $MyInvocation.MyCommand.Path
$wcrev = New-Object -ComObject "SubWCRev.object" #This requires TortoiseSVN
$wcrev.GetWCInfo($scriptPath, $false, $false)
$revision = $wcrev.Revision
$branch = ([System.Uri]$wcrev.Url).Segments[-1]

$inputFile = Join-Path $scriptPath VersionTemplate.cs
$outputFile = Join-Path $scriptPath Version.cs

Get-Content $inputFile | %{ $_ -replace "\`$WCREV\`$", $revision -replace "\`$WCBRANCH\`$", $branch } | Set-Content $outputFile