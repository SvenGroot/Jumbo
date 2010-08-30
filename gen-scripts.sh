#!/bin/sh
# $Id$
#
# Generates scripts for easy invocation of all Jumbo executables on unix shells.
monoExe=`which mono`
cd nantbin
for file in *.exe; do
    script=${file%.exe}
    echo \#!/bin/sh > $script
    echo $monoExe $file \$@ >> $script
    chmod +x $script
done