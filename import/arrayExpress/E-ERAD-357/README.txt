Downloaded .idf.txt and .sdrf files from ArrayExpress web site and convert to initial tagStorm
	arrayExpressToTagStorm E*.idf.txt E*.sdrf.txt ae.tags
	arrayExpressStormToHcaStorm ae.tags aeHca.tags

Convert to tagStorm for curation with
	tagStormToTab aeHca.tags aeHca.tsv
	tagStormFromTab aeHca.tsv uncurated.tags 
	cp uncurated.tags curated.tags

Edit curated.tags.  Added following tags:
    assay.rna.primer random
    assay.seq.molecule RNA
    assay.single_cell.method Fluidigm C1
    sample.body_part.cell_count 1
    sample.body_part.organ brain
    sample.body_part.name arcuate nucleus of hypothalamus
    sample.donor.ncbi_taxon 10090
    cell.type neuron
    cell.id

Finally add files and make bundles
	hcaStormToBundles curated.tags urls bundles


