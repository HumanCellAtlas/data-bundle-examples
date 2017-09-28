# Sample Queries

This Jupyter notebook contains some sample queries of Elasticsearch, the
HCA system, or/or the Boardwalk API.

## Install

You can find out how to install Jupyter [here](http://jupyter.readthedocs.org/en/latest/install.html) but I found it easier just to use Anaconda to install everything for me (the 3.x version).  See [Anaconda's](https://www.continuum.io/downloads) website.

You can also setup an environment before running jupyter using something like:

    conda create -n python3-sample-queries python=3.6 anaconda
    source activate python3-sample-queries

If you forgot the conda environment you setup previously use the below to list them:

    conda info --envs

And then continuing on with the launch of jupyter below.

## Running

Change to this directory in your shell then execute:

    jupyter notebook

You can then load the `.ipynb` file in the web GUI that opens in your default web browser.

We're using Python 3.6 in this notebook.
