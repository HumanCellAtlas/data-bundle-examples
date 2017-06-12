Downloaded .idf.txt and .sdrf files from ArrayExpress web site and convert to initial tagStorm
	arrayExpressToTagStorm E*.idf.txt E*.sdrf.txt ae.tags
	arrayExpressStormToHcaStorm ae.tags aeHca.tags

Convert to tagStorm for curation with
	tagStormToTab aeHca.tags aeHca.tsv
	tagStormFromTab aeHca.tsv uncurated.tags -div=project.title,sample.donor.id,assay.ena_experiment
	cp uncurated.tags curated.tags

Edit curated.tags.  Be sure to add assay.single_cell.method, assay.seq.molecule, and assay.seq.paired_end.  

Finally add files and make bundles
	hcaStormToBundles curated.tags urls bundles


