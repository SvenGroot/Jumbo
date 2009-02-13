#!/bin/bash

export JUMBO_TESTOUTPUT=/home2/sgroot/jumbo/TestOutput
mono ~/nunit/bin/nunit-console.exe -labels Tkl.Jumbo.Test.dll $@
