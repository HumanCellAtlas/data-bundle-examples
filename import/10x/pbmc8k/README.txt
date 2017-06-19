This is one of the demo datasets from 10X, see
https://support.10xgenomics.com/single-cell-gene-expression/datasets/pbmc8k

----

8k PBMCs from a Healthy Donor
Chromium Demonstration (v2 Chemistry) Dataset by Cell Ranger 1.3.0

Peripheral blood mononuclear cells (PBMCs) from a healthy donor (same donor as pbmc4k). PBMCs are primary cells with relatively small amounts of RNA (~1pg RNA/cell).

8,403 cells detected
Sequenced on Illumina Hiseq4000 with approximately 92,000 reads per cell
26bp read1 (16bp Chromium barcode and 10bp UMI), 98bp read2 (transcript), and 8bp I7 sample barcode
Analysis run with --cells=10000

----

Makedoc:

# download and extract
lftp -e 'pget -n 8 https://s3-us-west-2.amazonaws.com/10x.files/samples/cell-exp/pbmc8k/pbmc8k_fastqs.tar'
tar xvf pbmc8k_fastqs.tar

----
Created "curated.tags" by hand after examining 10x site. Then ran
    hcaStormToBundles curated.tags http://hgwdev.cse.ucsc.edu/~kent/hca/big_data_files/ bundles
