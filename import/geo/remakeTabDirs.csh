#!/bin/tcsh -efx
foreach i (GSE*)
	cd $i
	hcaAddSrrFiles curated.tags hcaTagStorm.tags
	rm -rf tabDir
	hcaStormToTabDir hcaTagStorm.tags tabDir
	cd ..
end

