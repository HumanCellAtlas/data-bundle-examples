#!/bin/bash

# downloads data files too big to store on github

# smartseq2

wget ftp://ftp.sra.ebi.ac.uk/vol1/fastq/ERR163/003/ERR1630013/ERR1630013.fastq.gz -O smartseq2/23bd7eb0-23a3-4898-b043-f7e982de281f.v1/ERR1630013.fastq.gz

# dropseq
wget http://hgwdev.soe.ucsc.edu/~kent/hca/array_express_examples/chosenSets/E-GEOD-81904/E-GEOD-81904.GSM2177575_Bipolar6.fastq.gz -O dropseq/13bd7eb0-23a3-4898-b043-f7e98223281g.v1/E-GEOD-81904.GSM2177575_Bipolar6.fastq.gz

# 10x
wget http://hgwdev-kent.cse.ucsc.edu/~kent/hca/10x_examples/chosenSets/pbmc8k/bundles/bundle1/pbmc8k_S1_L007_I1_001.fastq.gz -O 10X/pbmc8k_S1_L007_I1_001.fastq.gz
wget http://hgwdev-kent.cse.ucsc.edu/~kent/hca/10x_examples/chosenSets/pbmc8k/bundles/bundle1/pbmc8k_S1_L007_R1_001.fastq.gz -O 10X/pbmc8k_S1_L007_R1_001.fastq.gz
wget http://hgwdev-kent.cse.ucsc.edu/~kent/hca/10x_examples/chosenSets/pbmc8k/bundles/bundle1/pbmc8k_S1_L007_R2_001.fastq.gz -O 10X/pbmc8k_S1_L007_R2_001.fastq.gz
wget http://hgwdev-kent.cse.ucsc.edu/~kent/hca/10x_examples/chosenSets/pbmc8k/bundles/bundle1/pbmc8k_S1_L008_I1_001.fastq.gz -O 10X/pbmc8k_S1_L008_I1_001.fastq.gz
wget http://hgwdev-kent.cse.ucsc.edu/~kent/hca/10x_examples/chosenSets/pbmc8k/bundles/bundle1/pbmc8k_S1_L008_R1_001.fastq.gz -O 10X/pbmc8k_S1_L008_R1_001.fastq.gz
wget http://hgwdev-kent.cse.ucsc.edu/~kent/hca/10x_examples/chosenSets/pbmc8k/bundles/bundle1/pbmc8k_S1_L008_R2_001.fastq.gz -O 10X/pbmc8k_S1_L008_R1_001.fastq.gz
