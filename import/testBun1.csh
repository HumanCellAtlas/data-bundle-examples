#!/bin/tcsh -efx
foreach t (assay sample project)
    foreach i (`echo */*/bundles/bundle1/$t.json`)
	jsonschema -i $i /hive/groups/hca/metadata-standards/jsonschema/$t.json
    end
end
