#!/bin/tcsh -efx
foreach i (E-*)
	cd $i
	rm -rf tabDir
	hcaStormToTabDir curated.tags tabDir
	cd ..
end

