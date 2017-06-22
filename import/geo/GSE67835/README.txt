This is a CIRM dataset.  We have actually a little more info than the GEO record, so it gets
added in too, using the file cirm_extra.tsv

Downloaded soft file from NCBI web site to GSE67835_family.soft.gz and convert to 
	gunzip *.soft.gz
	geoToTagStorm *_family.soft geo.tags

Downloaded sra format DNA files with
	lftp  -c "mirror -P 20  ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByStudy/sra/SRP/SRP057/SRP057196 sra"
	chmod u+w sra/SRR* sra/SRR/*

Run parasol job to convert .sra files to .fastq.gz's

Download SraRunTable.txt by going to https://www.ncbi.nlm.nih.gov/sra?term=SRP057196 and selecting "send to" "Run Selector"
   and then RunInfo table button under Download,  and then copying file from
   laptop to this directory.  (How to do this programatically!?)
Convert this to a SRX/SRR two column file as so
        tabQuery "select Experiment_s,Run_s from SraRunTable.txt" > srxToSrr.tab


Convert to tagStorm for curation with
	geoStormToHcaStorm geo.tags srxToSrr.tab srr.tags
	tagStormJoinTab -append sample.geo_sample cirm_extra.tsv srr.tags joined.tags
	tagStormToTab joined.tags joined.tsv
	tagStormFromTab joined.tsv uncurated.tags -noHoist -div=project.title,sample.donor.id,sample.characteristics_c1_chip_id,assay.sra_experiment
	cp uncurated.tags curated.tags

Edit curated tags.  

Run this to add UUIDs
    hcaAddUuidToStorm curated.tags curated.tags

Then add files and convert to bundles
	hcaAddSrrFiles curated.tags hcaTagStorm.tags
	hcaStormToBundles hcaTagStorm.tags `pwd`/sra bundles

