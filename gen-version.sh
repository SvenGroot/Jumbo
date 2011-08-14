#!/bin/bash
scriptDir=$(dirname $0)
outputFile=AssemblyInfoCommon.cs
inputFile="$outputFile.template"
tempFile="$outputFile.temp"

let year=`date +%Y`
let year=(year-2011)%6
build="$year$(date +%m%d)"

revision=`svnversion -n | grep -o ^[0-9]*`
branch=`svn info | grep URL | grep -o -e "[^/]*$"`
cat $inputFile | sed -e 's/\$REVISION/'$revision'/' | sed -e 's/\$BRANCH/'$branch'/' | sed -e 's/\$BUILD/'$build'/' > $tempFile
if [ -f $outputFile ]; then
	diff $outputFile $tempFile > /dev/null
	if [ $? -eq 1 ]; then
		rm $outputFile
		mv $tempFile $outputFile
	else
		rm $tempFile
	fi
else
	mv $tempFile $outputFile
fi
