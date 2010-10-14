#!/bin/bash
if [ ! $NANT_PATH ]; then
	NANT_PATH=~/nant
fi
if [ ! $NUNIT_PATH ]; then
	NUNIT_PATH=~/nunit
fi

# NAnt itself doesn't run right under .Net 4.0, so we use 2.0 even though the target framework is 4.0
mono --runtime:v2.0.50727 $NANT_PATH/bin/NAnt.exe -t:mono-4.0 -D:nunitpath=$NUNIT_PATH -l:build.log $@