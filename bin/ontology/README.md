
# Scripts

Scripts for working with ontology aspects of metadata

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

Output will be written to output.txt containing the metadata field and value (tab separated)

```
E-MTAB-5061.sample.donor.sex	male
E-MTAB-5061.sample.donor.age_unit	year
E-MTAB-5061.sample.donor.species	Homo sapiens
E-MTAB-5061.assay.single_cell.method	FACS
E-MTAB-5061.sample.body_part.name	islet of Langerhans
E-MTAB-5061.sample.donor.sex	female
E-MTAB-5061.sample.donor.life_stage	adult
E-MTAB-5061.sample.body_part.organ	pancreas
```

* todo - this should read bundles from the datastore
* todo - at the moment fields are hard coded into the script, it may be useful to read these in from the json schema
