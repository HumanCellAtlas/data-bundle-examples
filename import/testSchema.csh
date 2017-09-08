#!/bin/tcsh -efx
foreach t (assay sample project)
    foreach i (`find . -name "$t.json" | randomLines stdin 10 stdout`)
	jsonschema -i $i /hive/groups/hca/metadata-standards/jsonschema/${t}.json
    end
end
