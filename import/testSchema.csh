#!/bin/tcsh -efx
# Since jsonschema is pretty slow, this just puts a random sample through
# json schema.

foreach t (assay sample project)
    foreach i (`find . -name "$t.json" | randomLines stdin 20 stdout`)
	jsonschema -i $i /hive/groups/hca/metadata-standards/jsonschema/${t}.json
    end
end
