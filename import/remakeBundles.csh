#!/bin/tcsh -efx
# This script calls hcaStormToBundles to convert curated.tags into a directory full of
# bundles for each data set.  Mostly it just delegates this to the remakeBundles.csh at
# the next level down in the directory hierarchy.  What you see here is some stuff to
# put the results into the ramdisk, at /dev/shm, where creating 100,000 small files is much
# faster.  It also tars up the result, and only this result, import.tgz is put in git.
pushd .
set destDir = /dev/shm/kent/hca/gitScratch
rm -rf $destDir
mkdir -p $destDir
tar -cvf $destDir/curatedTags.tar */*/curated.tags  */remakeBundles.csh curated.tight testSchema.csh testBun1.csh
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
