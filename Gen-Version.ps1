$outputName = "AssemblyInfoCommon.cs"
$inputFile = Join-Path $PSScriptRoot "$outputName.template"
$outputFile = Join-Path $PSScriptRoot $outputName

$year = ([DateTime]::Today.Year - 2000) % 6
$build = "$year" + [DateTime]::Today.ToString("MMdd")

$revision = 0
$branch = "git"

$newContent = Get-Content $inputFile | ForEach-Object { (($_ -replace "\`$BUILD", $build) -replace "\`$REVISION", $revision) -replace "\`$BRANCH", $branch }
$newContent
$needUpdate = $true
if( Test-Path $outputFile )
{
    # Only update the file if the contents have changed so we don't cause unnecessary rebuilds
    $needUpdate = $false
    $oldContent = Get-Content $outputFile
    if( $oldContent -eq $null -or (Compare-Object $oldContent $newContent) )
        { $needUpdate = $true }
}

if( $needUpdate )
{
    Write-Host "Updating $outputName"
    $newContent | Set-Content $outputFile
}
