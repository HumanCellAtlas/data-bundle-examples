import glob, json
from optparse import OptionParser

# Pull out values from all bundles that require an ontology annotation.
# Requires path to extracted bundles from github data-bundle-examples repo.

parser = OptionParser()
parser.add_option("-p", "--path", dest="path",
                  help="path to HCA example data bundles", metavar="FILE")

(options, args) = parser.parse_args()

path = options.path
if not path:
    print "You must supply path to the HCA bundles directory"
    exit(2)

ontoValues = {}

def getKey(projectId, object,objectType, level1):
    if level1 in object:
        key = projectId+"."+objectType+"."+level1+"."+object[level1]
        return [key, object[level1]]
    return []

def getNestedKey(projectId, object,objectType, level1, level2):
    if level1 in object:
        if level2 in object[level1]:
            key = projectId+"."+objectType+"."+level1+"."+level2+"."+object[level1][level2]
            return [key, object[level1][level2]]
    return []

projectId = "noid"

for dir in glob.glob(path+"/bundles/bundle*"):

    projectRaw = json.load(open (dir+"/project.json"))
    assayRaw = json.load(open (dir+"/assay.json"))
    manifestRaw = json.load(open (dir+"/manifest.json"))
    sampleRaw = json.load(open (dir+"/sample.json"))
    cellRaw = json.load(open (dir+"/cell.json"))

    projectId = projectRaw["id"]

    list = getNestedKey(projectId, assayRaw, "assay", "single_cell", "method")
    if list:
        ontoValues[list[0]]= list[1]

    list = getNestedKey(projectId, sampleRaw, "sample", "body_part", "organ")
    if list:
        ontoValues[list[0]]= list[1]

    list = getNestedKey(projectId, sampleRaw, "sample", "body_part", "name")
    if list:
        ontoValues[list[0]]= list[1]

    list = getKey(projectId, sampleRaw, "sample", "cell_line")
    if list:
        ontoValues[list[0]]= list[1]

    list = getNestedKey(projectId, sampleRaw, "sample", "donor", "age_unit")
    if list:
        ontoValues[list[0]]= list[1]

    list = getNestedKey(projectId, sampleRaw, "sample", "donor", "cause_of_death")
    if list:
        ontoValues[list[0]]= list[1]

    list = getNestedKey(projectId, sampleRaw, "sample", "donor", "development_stage")
    if list:
        ontoValues[list[0]]= list[1]

    list = getNestedKey(projectId, sampleRaw, "sample", "donor", "life_stage")
    if list:
        ontoValues[list[0]]= list[1]

    list = getNestedKey(projectId, sampleRaw, "sample", "donor", "species")
    if list:
        ontoValues[list[0]]= list[1]

    list = getNestedKey(projectId, sampleRaw, "sample", "donor", "ancestry")
    if list:
        ontoValues[list[0]]= list[1]

    list = getNestedKey(projectId, sampleRaw, "sample", "donor", "sex")
    if list:
        ontoValues[list[0]]= list[1]

    list = getNestedKey(projectId, sampleRaw, "sample", "donor", "medication")
    if list:
        ontoValues[list[0]]= list[1]

    list = getKey(projectId, sampleRaw, "sample", "protocol_type")
    if list:
        ontoValues[list[0]]= list[1]

    list = getKey(projectId, sampleRaw, "sample", "submitted_cell_type")
    if list:
        ontoValues[list[0]]= list[1]

file  = open(projectId+".csv", "w")

for key in ontoValues:
    subKey =  key[:key.rindex('.')]
    file.write(subKey + "\t" + ontoValues[key] + "\n")

file.close()