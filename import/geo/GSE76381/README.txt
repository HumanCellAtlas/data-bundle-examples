Downloaded soft file from NCBI web site to GSE76381_family.soft and convert to 
	geoToTagStorm *_family.soft geo.tags

Downloaded sra format DNA files with
	lftp  -c "mirror -P 20  ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByStudy/sra/SRP/SRP067/SRP067844 sra"
	chmod u+w sra/SRR* sra/SRR/*

Run parasol job to convert .sra files to .fastq.gz's

Download SraRunTable.txt by going to https://www.ncbi.nlm.nih.gov/sra?term=SRP067844 and selecting "send to" "Run Selector"
   and then RunInfo table button under Download,  and then copying file from
   laptop to this directory.  (How to do this programatically!?)
Convert this to a SRX/SRR two column file as so
        tabQuery "select Experiment_s,Run_s from SraRunTable.txt" > srxToSrr.tab


Convert to tagStorm for curation with
	geoStormToHcaStorm geo.tags srxToSrr.tab srr.tags
	tagStormToTab srr.tags srr.tsv
	tagStormFromTab srr.tsv foo1.tags -noHoist -div=project.title,sample.donor.species,sample.short_label,sample.donor.age,sample.geo_sample t
	tagStormJoinTab sample.geo_sample plate.tsv foo1.tags foo2.tags -append
	tagStormJoinTab sample.geo_sample sample.geo_sample gsm_srx_srr.tsv foo2.tags foo3.tags -append
	tagStormToTab foo3.tags foo3.tsv
	tagStormFromTab foo3.tsv uncurated.tags -noHoist -div=project.title,sample.donor.species,sample.short_label,sample.donor.age,sample.characteristics_plate,sample.geo_sample 
	cp uncurated.tags curated.tags

Edit curated tags.  

Add uuids with:  
	hcaAddUuidToStorm curated.tags curated.tags

Then add files and convert to bundles
	hcaAddSrrFiles curated.tags hcaTagStorm.tags
	hcaStormToBundles hcaTagStorm.tags `pwd`/sra bundles

