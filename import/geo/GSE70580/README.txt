Downloaded soft file from NCBI web site to GSE70580_family.soft and convert to 
	geoToTagStorm *_family.soft geo.tags

Downloaded sra format DNA files with
	lftp  -c "mirror -P 20 ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByStudy/sra/SRP/SRP060/SRP060416 sra"
	chmod u+w sra/SRR* sra/SRR/*

Run parasol job to convert .sra files to .fastq.gz's

Download SraRunTable.txt by going to https://www.ncbi.nlm.nih.gov/sra?term=SRP060416 and selecting "send to" "Run Selector"
   and then RunInfo table button under Download,  and then copying file from
   laptop to this directory.  (How to do this programatically!?)
Convert this to a SRX/SRR two column file as so
        tabQuery "select Experiment_s,Run_s from SraRunTable.txt" > srxToSrr.tab


Convert to tagStorm for curation with
	geoStormToHcaStorm geo.tags srxToSrr.tab srr.tags
	tagStormToTab srr.tags srr.tsv
	tagStormFromTab srr.tsv uncurated.tags -noHoist -div=project.title,sample.characteristics_tonsil_donor,sample.characteristics_facs_gating,assay.sra_experiment 
	cp uncurated.tags curated.tags

Edit curated tags.  

Add uuids with:  
	hcaAddUuidToStorm curated.tags curated.tags

Then add files and convert to bundles
	hcaAddSrrFiles curated.tags hcaTagStorm.tags
	hcaStormToBundles hcaTagStorm.tags `pwd`/sra bundles

