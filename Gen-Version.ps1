$scriptPath = Split-Path $MyInvocation.MyCommand.Path
$outputName = "AssemblyInfoCommon.cs"
$inputFile = Join-Path $scriptPath "$outputName.template"
$outputFile = Join-Path $scriptPath $outputName

$year = ([DateTime]::Today.Year - 2000) % 6
$build = "$year" + [DateTime]::Today.ToString("MMdd")

# Use 0 as revision for non-release builds to speed up debug builds
Write-Host "Determining working copy revision"
$wcrev = New-Object -ComObject "SubWCRev.object" #This requires TortoiseSVN
$wcrev.GetWCInfo($scriptPath, $false, $false)
$revision = $wcrev.Revision
$branch = ([System.Uri]$wcrev.Url).Segments[-1]
Write-Host "Revision number is $revision on branch $branch"

$newContent = Get-Content $inputFile | ForEach-Object { (($_ -replace "\`$BUILD", $build) -replace "\`$REVISION", $revision) -replace "\`$BRANCH", $branch }
$needUpdate = $true
if( Test-Path $outputFile )
{
    # Only update the file if the contents have changed so we don't cause unnecessary rebuilds
    $needUpdate = $false
    $oldContent = Get-Content $outputFile
    if( Compare-Object $oldContent $newContent )
        { $needUpdate = $true }
}

if( $needUpdate )
{
    Write-Host "Updating $outputName"
    $newContent | sc $outputFile
}


$inputFile = Join-Path $scriptPath VersionTemplate.cs
$outputFile = Join-Path $scriptPath Version.cs