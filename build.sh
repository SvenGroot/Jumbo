#!/bin/sh
# NAnt building is only for use on Linux.
# On Windows, please use MSBuild or Visual Studio.
scriptDir=$(dirname $0)
cd $scriptDir
if pkgconfig mono --atleast-version 3.0; then
	# The regular mono-4.0 target in NAnt 0.92 does not work for Mono 3.0 because gmcs is no longer an executable, but an alias for mcs.exe
	# Therefore, I have defined a new target (mono-4.5) that works properly in NAnt.exe.config
	echo "Building for Mono 3.0 or newer."
	mono tools/nant/bin/NAnt.exe -t:mono-4.5 -l:build.log $@
else
	mono $NANT_PATH/bin/NAnt.exe -t:mono-4.0 -l:build.log $@
fi
