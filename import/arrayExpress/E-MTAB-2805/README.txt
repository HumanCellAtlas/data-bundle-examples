Downloaded .idf.txt and .sdrf files from ArrayExpress web site and convert to initial tagStorm
	arrayExpressToTagStorm E*.idf.txt E*.sdrf.txt ae.tags
	arrayExpressStormToHcaStorm ae.tags aeHca.tags

Convert to tagStorm for curation with
	tagStormToTab aeHca.tags aeHca.tsv
	tagStormFromTab aeHca.tsv uncurated.tags 
	cp uncurated.tags curated.tags

Edit curated.tags.  Added following tags:
    assay.single_cell.method Fluidigm C1
    assay.seq.molecule RNA
    assay.rna.primer random
    assay.rna.spike_in ERCC
    sample.treatment.culture_type cell line
    sample.body_part.cell_count
    sample.cell_line AB2.2
    sample.storage fresh
    sample.body_part.name embryonic stem cell
    cell.type embyronic stem cell
and ran
    hcaAddUuidToStorm curated.tags curated.tags

Be sure to add assay.single_cell.method, assay.seq.molecule, and assay.seq.paired_end.  

Finally add files and make bundles
	hcaStormToBundles curated.tags urls bundles


