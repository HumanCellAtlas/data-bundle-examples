Downloaded sra format DNA files with
	lftp  -c "mirror -P 20  ftp://ftp-trace.ncbi.nih.gov/sra/sra-instant/reads/ByStudy/sra/SRP/SRP075/SRP075719/ sra"
	chmod u+w sra/SRR* sra/SRR/*

Run parasol job to convert .sra files to .fastq.gz's

Downloaded soft file from NCBI web site to GSE81904_family.soft and convert to 
	geoToTagStorm *_family.soft geo.tags

Download SraRunTable.txt by going to https://www.ncbi.nlm.nih.gov/sra?term=SRP075719 and selecting "send to" "Run Selector"
   and then RunInfo table button under Download,  and then copying file from
   laptop to this directory.  (How to do this programatically!?)
Convert this to a SRX/SRR two column file as so
        tabQuery "select Experiment_s,Run_s from SraRunTable.txt" > srxToSrr.tab


Convert to tagStorm for curation with
	geoStormToHcaStorm geo.tags srxToSrr.tab srr.tags
	tagStormToTab srr.tags srr.tsv
	tagStormFromTab srr.tsv uncurated.tags
	cp uncurated.tags curated.tags

Edit curated tags.  Be sure to add assay.single_cell.method, assay.seq.molecule, and assay.seq.paired_end.  Then add files
and convert to bundles
	hcaAddSrrFiles curated.tags hcaTagStorm.tags
	hcaStormToBundles hcaTagStorm.tags `pwd`/sra bundles

