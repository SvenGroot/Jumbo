#!/bin/sh
sed -e 's/\$WCREV\$/'`svnversion -n jumbo/src | grep -o ^[0-9]*`'/' VersionTemplate.cs > Version.cs