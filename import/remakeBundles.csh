#!/bin/tcsh -efx
pushd .
set destDir = /dev/shm/kent/hca/gitScratch
rm -rf $destDir
mkdir -p $destDir
tar -cvf $destDir/curatedTags.tar */*/curated.tags  */remakeBundles.csh curated.tight testSchema.csh
cd $destDir
mkdir import
cd import
tar -xvf ../curatedTags.tar
foreach i (10x arrayExpress geo)
    cd $i
    ./remakeBundles.csh
    cd ..
end
cd ..
pwd
tar -czf import.tgz import/*/*/bundles/bun*/*.json
popd 
cp $destDir/import.tgz .
echo made import.tgz - see $destDir/import for unpacked version
