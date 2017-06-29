#!/bin/tcsh -efx
foreach i (E-*)
	cd $i
	hcaStormToBundles curated.tags urls ../../curated.tight bundles
	cd ..
end

