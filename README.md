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

According to Array Express: "Libraries were sequenced on an Illumina HiSeq 2000, generating 43 bp single-end reads." So I believe the single file is correct.

## dropseq

This is E-GEOD-81904.  This is based on Jim's example, see his google doc [here](https://docs.google.com/document/d/1CNFGWxsrrc8vVn4PBsojfb1aAM_dkwfHrJW0vjR_ulU/edit).

Jim made several bundles (one per sample), we're just storing the first one in this repo that corresponds to a single sample and multiple cells.

NOTE: we need to get the real fastq files

## 10x

This is based on Jim's example, see his google doc [here](https://docs.google.com/document/d/1iu03FdjLH9TjDS3XN135l0G1sSmsnR6tS7_Kp2yMkEg/edit).

See https://support.10xgenomics.com/single-cell-gene-expression/datasets/pbmc8k


## TODO & Questions for the Group
* the analysis.json files need to be redone to show an upload not an alignment
* we need to check the fastq files, I don't think they are correct since we expect multiple fastq files per data bundle.
    * smartseq2 I think is correct since it's a single-end experiment
    * dropseq I think is missing the fastq1 file since it was converted from BAM, so this is lost?
* we have a cell.json and sample.json... do we need both? Laura and Tim think it's overlapping for sample and should just use sample.json.
* where does quality control for a release go?
* what about samples being run multiple times (multiple lanes)?  Do they get individual data bundles or a single data bundle which has been combined?  
