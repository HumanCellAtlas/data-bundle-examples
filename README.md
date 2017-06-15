# data-bundle-examples v1

## About

A repository housing metadata and data files (or links to data files) that are being prepared as sample data bundles for various uses.

Hand curated examples can be found in the root directory each in their own directory named for the platform.  Bulk imported examples can be found in the `import` directory.

Since thousands of JSON files were produced in the import subdirectories, they have been removed from Git and included as a single compressed tarball `import.tgz`.

## Extract Metadata Files

This command will extract the metadata JSON files associated with the `import` examples.  This will create thousands of files so please **do not** check them into Git:

    tar zxf import.tgz

## Get Data Files

Downloads the fastq files associated with each hand-curated example:

    bash bin/get_data.sh

For the import directory structure, first extract the metadata files (see above) and then run:

    python bin/get_import_data.py

## Smartseq2

This is E-MTAB-5061.  This is based on Jim's example, see his google doc [here](https://docs.google.com/document/d/1QSd_nnTUFSBMUnXvlva8ouzcuX5s8ljeBCLXU4afpQs/edit).

Jim made several hundred sample bundles, we're just storing the first one in this repo that corresponds to a single cell.

According to Array Express: "Libraries were sequenced on an Illumina HiSeq 2000, generating 43 bp single-end reads." So I believe the single file is correct.


## Drop-seq

This is GSE81904.  This is based on Jim's example, see his google doc [here](https://docs.google.com/document/d/1CNFGWxsrrc8vVn4PBsojfb1aAM_dkwfHrJW0vjR_ulU/edit).

* Jim made several bundles (one per sample), we're just storing the first one in this repo that corresponds to a single sample and multiple cells.


## 10X

This is based on Jim's example, see his google doc [here](https://docs.google.com/document/d/1iu03FdjLH9TjDS3XN135l0G1sSmsnR6tS7_Kp2yMkEg/edit).

See https://support.10xgenomics.com/single-cell-gene-expression/datasets/pbmc8k

## Import

Larger scale projects imported in full from ArrayExpress, GEO, etc.  The json bundles for these are in import.tgz, and include
more than 30,000 files.  Do a tar -xf import.tgz to unpack, preferably in a ram-disk.  These are json files are generated from
the tagStorm format curated.tags file in sub-sub directories of the import subdirectory.  

See http://hgwdev.soe.ucsc.edu/~kent/hca/projects.html for a list of the projects involved.

## Sources of Sample Metadata/Data

Jim and Laura are both preparing sample data bundles along with other Green/Blue/Purple box team members.  Here are some sources of samples that should be consolidated here:

* Google drive with sample data bundles writeup docs from Jim: https://drive.google.com/drive/u/1/folders/0BygSqPRIIdoSMVJVRWEyY0xWeXM
* Data bundles from Jim, referenced in the previous doc: http://hgwdev-kent.cse.ucsc.edu/~kent/hca/array_express_examples/chosenSets/
* Metadata fields from Jim: https://docs.google.com/spreadsheets/d/1LXIs2kM2MLTwKSpKo3Pt8-EFsyOEKwZ0QX4PlkbBucw/edit#gid=0
* from Laura: https://www.ebi.ac.uk/ena/portal/api/search?result=read_run&format=JSON&limit=0&fields=study_accession,sample_accession,experiment_alias,experiment_title,fastq_ftp,fastq_md5,fastq_bytes,last_updated&query=run_accession=ERR1630017

## TODO & Questions for the Group

### Drop-seq
1. UMI offset, UMI size,cell barcode size, cell barcode offset should not change for the assay Drop-seq (or the 10X version assay). It is possible to minimize the metadata here given they are all realistically one unit of information. It depends on the purpose of the json file just FYI [from Tim].
     * True.  I could drop these, but as assay methods proliferate I thought it might be nice to have these spelled out. [from Jim].
2. Need to add 10X channel information on top of sequencer lane information for this [from Tim].
     * I'm not quite sure what you mean by this. [from Jim]
3. Need to add lane information to this like you have in 10X [from Tim].
     * OK. Everything has lanes now. [from Jim]

### 10X
1. This is documented as an example of v2 chemistry, do we want v1 and V(D)J? If so are you going to grab the data or do we want to simulate from this data set [from Tim]?
    * I can grab a 10x v1, which I think would be good to have.  The V(D)J is sort of specialized.  I'd like to skip it for now. [from Jim]

2. If you are going to have a type for fastq file to differentiate the file with the transcript then this generalizes to Drop-Seq and you should do that too, you could also do this to Smartseq2 (both transcript) if you want to keep the pattern standard [from Tim].
    8 I've got it set up for both drop-seq and 10x_v2 to use type=index and type=reads  [from Jim]
3. Need to include the barcodes used for the 10X run (there are different library barcodes one can use) [from Tim].
    * Hmm.  There is and I.fastq.gz file (or is it I3.fastq.gz) file that has the observed sample library barcodes.
      I wonder if that's what you mean.  Otherwise I'm not sure where to find it. I could parse it out of their
      matrix I suppose, but maybe it's somewhere pre-alignment. [from Jim]

### General
1. The analysis.json files need to be redone to show an upload not an alignment.
    * What do you mean by this? Analysis.json (now provenance.json) are generated after a green run not a purple upload [from Tim].
2. We need to check the fastq files, I don't think they are correct since we expect multiple fastq files per data bundle.
    * Smartseq2 I think is correct since it's a single-end experiment
        * this is not standard, Smartseq2 is expected to be paired sequencing [from Tim]
	* I've got both single and paired end examples now under smartseq2 [from Jim]
    * Drop-seq I think is missing the fastq1 file since it was converted from BAM, so this is lost?
        * Agreed, this would happen if the bam was post alignment, pre-annotation [from Tim].
        * I have some files for Smartseq2 and Drop-seq, where can I put them for the get_data.sh to pull. Also have associated output files that were ran on pipelines from the input data. It would be great to wget these files not to the data folder but into thier respective bundles [from Tim].
	* This is corrected now.  I couldn't find the fastqs in array express, but the experiment is also in GEO. [from Jim].
3. We have a cell.json and sample.json... do we need both? Laura and Tim think it's overlapping for sample and should just use sample.json.
    * Agreed, moved to an attic space for now [from Tim].
    * Cell I kept since it does contain unique info, but stuff unique to cell-at-a-time assays.  I kind of expect it'll get reworked
      and for now the pipeline can just ignore it.  The red box likely will want this later if it exists. [from Jim]
4. Where does quality control for a release go?
    * Would we want the release to be a bundle that contains the products of the release process (in line with our handling of green runs)? I would like to see in the release a file manifest, indication of white/grey/black listing, information about the criteria to be in each listing (because this may change between releases), time/date info [from Tim].
    * I would like to see the quality info go in a qc_stats.json file. [from Jim]
5. What about samples being run multiple times (multiple lanes)?  Do they get individual data bundles or a single data bundle which has been combined?
    * My current thinking is that I feel this is best served by updating a bundle but not making a new one [Tim].
6. What will be the input file format expected in the system? Are we going to start with fastq.gz file uploads or something else? [from Tim]
    * At the moment I'm making them all fastq.gz.  I'm converting the .sra format to this for GEO accessions.  The 10x examples were tar'd
      fastq.gzs and I untarred them.  For ArrayExpress so far they have had .fastq.gz already available from URL, so no conversion there.
      I hear they are wanting to convert to CRAM.  The one I saw with CRAM for alignments did also have .fastq.gz though. [from Jim]
7. Do we want to store the expression matrices in a format more usable for sparse data [from Tim]?
    * I am inclinded to say yes [Tim].
    * The 10x have a hca5 based format I'd sort of like to stay away from. [form Jim]
