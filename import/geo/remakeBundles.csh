#!/bin/tcsh -efx
foreach i (GSE*)
    cd $i
    hcaAddSrrFiles curated.tags hcaTagStorm.tags
    hcaStormToBundles hcaTagStorm.tags http://hgwdev.cse.ucsc.edu/~kent/hca/big_data_files ../../curated.tight bundles
    cd ..
end

