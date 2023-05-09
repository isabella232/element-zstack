# Data Pipeline

Element ZStack is composed of two main schemas, `volume` and `volume_matching`. Data 
export to BossDB is handled with a `bossdb` schema and upload utilities.

- `volume` module - performs segmentation of volumetric microscopic images with 
`cellpose`.

- `volume_matching` module - performs volume registration to a common space and matches 
cells across imaging sessions.

- `bossdb` module - uploads data to BossDB, creates a Neuroglancer visualization, and 
stores the relevant URLs.

Each node in the following diagram represents the analysis code in the pipeline and the
corresponding table in the database.  Within the workflow, Element ZStack
connects to upstream Elements including Lab, Animal, Session, and Calcium Imaging. For 
more detailed documentation on each table, see the API docs for the respective schemas.

![pipeline](https://raw.githubusercontent.com/datajoint/element-zstack/images/pipeline.svg)

