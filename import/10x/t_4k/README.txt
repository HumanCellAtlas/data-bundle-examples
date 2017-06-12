This is one of the demo datasets from 10X, see
https://support.10xgenomics.com/single-cell-gene-expression/datasets/t_4k

----

Pan T cells isolated from mononuclear cells of a healthy donor (same donor as t_3k). T cells are primary cells with relatively small amounts of RNA (~1pg RNA/cell).

4,518 cells detected
Sequenced on Illumina Hiseq4000 with approximately 73,000 reads per cell
26bp read1 (16bp Chromium barcode and 10bp UMI), 98bp read2 (transcript), and 8bp I7 sample barcode
Analysis run with --cells=6000
Published on February 9, 2017

This dataset is licensed under the Creative Commons Attribution license.

----

Makedoc:

# download and extract
lftp -e 'pget -n 8 https://s3-us-west-2.amazonaws.com/10x.files/samples/cell-exp/t_4k/t_4k_fastqs.tar'
tar xvf t_4k_fastqs.tar

----
Created "curated.tags" by hand after examining 10x site. Then ran
    hcaStormToBundles curated.tags http://hgwdev.cse.ucsc.edu/~kent/hca/big_data_files/ bundles
