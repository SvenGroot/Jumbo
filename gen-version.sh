#!/bin/sh
revision=`svnversion -n | grep -o ^[0-9]*`
branch=`svn info | grep URL | grep -o -e "[^/]*$"`
cat VersionTemplate.cs | sed -e 's/\$WCREV\$/'$revision'/' | sed -e 's/\$WCBRANCH\$/'$branch'/' > Version.cs