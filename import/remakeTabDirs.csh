#!/bin/tcsh -efx
foreach i (10x arrayExpress geo)
    cd $i
    ./remakeTabDirs.csh
    cd ..
end
