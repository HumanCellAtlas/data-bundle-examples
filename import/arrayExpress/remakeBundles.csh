#!/bin/tcsh -efx
foreach i (E-*)
	cd $i
	hcaStormToBundles curated.tags urls bundles
	cd ..
end

