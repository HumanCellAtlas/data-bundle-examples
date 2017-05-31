# data-bundle-examples

## About

A repository housing metadata and data files (or links to data files) that are being prepared as sample data bundles for various uses.

## Sources of Sample Metadata/Data

Jim and Laura are both preparing sample data bundles along with other Green/Blue/Purple box team members.  Here are some sources of samples that should be consolidated here:

* Google drive with sample data bundles writeup docs from Jim: https://drive.google.com/drive/u/1/folders/0BygSqPRIIdoSMVJVRWEyY0xWeXM
* Data bundles from Jim, referenced in the previous doc: http://hgwdev-kent.cse.ucsc.edu/~kent/hca/array_express_examples/chosenSets/
* Metadata fields from Jim: https://docs.google.com/spreadsheets/d/1LXIs2kM2MLTwKSpKo3Pt8-EFsyOEKwZ0QX4PlkbBucw/edit#gid=0
* from Laura: https://www.ebi.ac.uk/ena/portal/api/search?result=read_run&format=JSON&limit=0&fields=study_accession,sample_accession,experiment_alias,experiment_title,fastq_ftp,fastq_md5,fastq_bytes,last_updated&query=run_accession=ERR1630017

## Get Data Files

Downloads the fastq files associated with each example:

    bash bin/get_data.sh

## smartseq2

This is E-MTAB-5061.  This is based on Jim's example, see his google doc [here](https://docs.google.com/document/d/1QSd_nnTUFSBMUnXvlva8ouzcuX5s8ljeBCLXU4afpQs/edit).

Jim made several hundred sample bundles, we're just storing the first one in this repo that corresponds to a single cell.

## dropseq

This is E-GEOD-81904.  This is based on Jim's example, see his google doc [here](https://docs.google.com/document/d/1CNFGWxsrrc8vVn4PBsojfb1aAM_dkwfHrJW0vjR_ulU/edit).

Jim made several bundles (one per sample), we're just storing the first one in this repo that corresponds to a single sample and multiple cells.

 
