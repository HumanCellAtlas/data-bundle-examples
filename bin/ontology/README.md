
# Scripts

Scripts for working with ontology aspect of metadata

## Extract ontology values

This script is used to extract all values from all fields in data bundles that require ontology annotation. This will allow the ontologists to easily access all
values that require ontology annotation to assess coverage.

To run the script you will need to checkout the data-bundle-examples repo and unzip the imports.zip to unpack all the example bundles. Run the script for each bundle by providing the path to the directory containing the "bundles" directory. 

```
python extract_ontology_values.py -p <path to bundles>
```

Example

```
python extract_ontology_values.py -p ~/hca/data-bundle-examples/import/import/arrayExpress/E-MTAB-5061
```

Output

Tab seperated output 

* todo - this should read bundles from the datastore
* todo - at the moment fields are hard coded into the script, it may be useful to read these in from the json schema