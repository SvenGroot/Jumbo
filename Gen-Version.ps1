$scriptPath = Split-Path $MyInvocation.MyCommand.Path
$inputFile = Join-Path $scriptPath AssemblyInfoCommon.cs.template
$outputFile = Join-Path $scriptPath AssemblyInfoCommon.cs

$year = ([DateTime]::Today.Year - 2000) % 6
$build = "$year" + [DateTime]::Today.ToString("MMdd")

# Use 0 as revision for non-release builds to speed up debug builds
Write-Host "Determining working copy revision"
$wcrev = New-Object -ComObject "SubWCRev.object" #This requires TortoiseSVN
$wcrev.GetWCInfo($scriptPath, $false, $false)
$revision = $wcrev.Revision
$branch = ([System.Uri]$wcrev.Url).Segments[-1]
Write-Host "Revision number is $revision on branch $branch"

$newContent = Get-Content $inputFile | foreach { (($_ -replace "\`$BUILD", $build) -replace "\`$REVISION", $revision) -replace "\`$BRANCH", $branch }
$needUpdate = $true
if( Test-Path $outputFile )
{
    # Only update the file if the contents have changed so we don't cause unnecessary rebuilds
    $needUpdate = $false
    $oldContent = Get-Content $outputFile
    if( $newContent.Length -ne $oldContent.Length )
        { $needUpdate = $true }
    else
    {
        for( $x = 0; $x -lt $newContent.Length; $x++ )
        {
            if( $oldContent[$x] -ne $newContent[$x] )
            {
                $needUpdate = $true
                break
            }
        }
    }
}

if( $needUpdate )
{
    Write-Host "Updating AssemblyInfoCommon.cs"
    $newContent | sc $outputFile
}


$inputFile = Join-Path $scriptPath VersionTemplate.cs
$outputFile = Join-Path $scriptPath Version.cs