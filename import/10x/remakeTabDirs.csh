#!/bin/tcsh -efx
foreach i (pbmc8k t_4k)
	cd $i
	rm -rf tabDir
	hcaStormToTabDir curated.tags tabDir
	cd ..
end

