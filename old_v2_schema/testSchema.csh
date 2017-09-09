#!/bin/tcsh -efx
set t = cell
foreach t (assay cell sample project)
    foreach i (`find . -name "$t.json" | randomLines stdin 10 stdout`)
	jsonschema -i $i /hive/groups/hca/git/schema/${t}_schema.json
    end
end
