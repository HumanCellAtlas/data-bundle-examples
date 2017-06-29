#!/bin/tcsh -efx
foreach i (pbmc8k t_4k)
	cd $i
	hcaStormToBundles curated.tags http://hgwdev.cse.ucsc.edu/~kent/hca/big_data_files ../../curated.tight bundles
	cd ..
end

