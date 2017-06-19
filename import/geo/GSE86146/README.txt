Downloaded soft file from NCBI web site to GSE90806_family.soft and convert to 
	geoToTagStorm *_family.soft geo.tags

Downloaded sra format DNA files with
	lftp  -c "mirror -P 20 ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByStudy/sra/SRP/SRP083/SRP083134 sra"
	chmod u+w sra/SRR* sra/SRR/*

Run parasol job to convert .sra files to .fastq.gz's

Download SraRunTable.txt by going to https://www.ncbi.nlm.nih.gov/sra?term=SRP083134 and selecting "send to" "Run Selector"
   and then RunInfo table button under Download,  and then copying file from
   laptop to this directory.  (How to do this programatically!?)
Convert this to a SRX/SRR two column file as so
        tabQuery "select Experiment_s,Run_s from SraRunTable.txt" > srxToSrr.tab


Convert to tagStorm for curation with
	geoStormToHcaStorm geo.tags srxToSrr.tab srr.tags
	tagStormToTab srr.tags srr.tsv
	tagStormFromTab srr.tsv uncurated.tags -div=project.title,sample.donor.life_stage,sample.geo_sample
	cp uncurated.tags curated.tags

Edit curated tags.  Added at start, replacing existing root level values:
    assay.rna.prep modified smart-seq2
    assay.rna.primer random
    assay.single_cell.method mouth pipette
    assay.seq.umi_barcode_size 8
    assay.single_cell.cell_barcode_size 8
    sample.donor.age_unit week
    sample.donor.life_stage embryo
Replaced
    sample.characteristics_gender  with sample.donor.sex
    sample.long_label sample.donor.id
Hand edited in with help of tagStormJoinTab
    sample.donor.id
    sample.donor.age
    sample.

Add uuids with:  
	hcaAddUuidToStorm curated.tags curated.tags

Then add files and convert to bundles
	hcaAddSrrFiles curated.tags hcaTagStorm.tags
	hcaStormToBundles hcaTagStorm.tags `pwd`/sra bundles

