# Concepts

Volumetric calcium imaging is a relatively new technique in neuroscience but advances in
computational capabilities have facilitated a rapid rise in its development and
use. As more labs use volumetric calcium imaging to
understand the brain, standardization of annotations and raw data storage have
emerged as some of the greatest challenges in the field. 

This element consists of a pipeline to perform volume matching across sessions
and cell segmentation using `cellpose` for volumetric microscopic calcium imaging data, and an
interface to easily upload the results and raw data to the Brain Observatory
Storage Service and Database (BossDB).

## Key Partnerships

Over the several years, many labs have developed DataJoint-based pipelines for volumetric
data. The DataJoint team interviewed and collaborated these teams to
understand their experiment workflow, associated tools, and interfaces. These teams
include:

- Andreas Tolias Lab, Baylor College of Medicine
- Applied Physics Laboratory, Johns Hopkins University

## Element Features

Through our interviews and direct collaboration with the Applied Physics Laboratory, we identified
the common motifs to create the Element ZStack with the repository hosted at
https://github.com/datajoint/element-zstack.

Major features of Element ZStack include:

- Ingest metadata for a stitched volumetric microscopic calcium-imaging data
  file.
- Volume matching for tracking volume data across multiple acquisition sessions.
- Perform segmentation using on volumetric data using cellpose. 
- Schemas to upload data to BossDB and store the BossDB URL in the database.
- Generate a volume visualization link through Neuroglancer and store the URL in
  the database.


## Element architecture

Each node in the following diagram represents the analysis code in the workflow and the
corresponding table in the database.  Within the workflow, Element ZStack
connects to upstream Elements including Lab, Animal, and Session. For more detailed
documentation on each table, see the API docs for the respective schemas.

The Element is composed of two main schemas, `volume` and `volume_matching`. To handle
data export and storage in BossDB we have also designed the `bossdb` schema and
created upload utilities within this Element. 

- `volume` module - performs segmentation on volumetric microscopic imaging
  data.

- `volume_matching` module - performs volume matching to track volumes across sessions.

- `bossdb` module - uploads data to BossDB and stores the relevant URLs.

![element zstack diagram](https://raw.githubusercontent.com/datajoint/element-zstack/images/zstack_diagram.svg)

### `lab` schema ([API docs](../api/workflow_calcium_imaging/pipeline/#workflow_calcium_imaging.pipeline.Equipment))

| Table | Description |
| --- | --- |
| Equipment | Scanner metadata |

### `subject` schema ([API docs](https://datajoint.com/docs/elements/element-animal/api/element_animal/subject))

- Although not required, most choose to connect the `Session` table to a `Subject` table.

| Table | Description |
| --- | --- |
| Subject | Basic information of the research subject |

### `session` schema ([API docs](https://datajoint.com/docs/elements/element-session/api/element_session/session_with_datetime))

| Table | Description |
| --- | --- |
| Session | Unique experimental session identifier |

### `volume` schema ([API docs](https://datajoint.com/docs/elements/element-zstack/api/element_zstack/volume))

| Table | Description |
| --- | --- |
| Volume | Details about the volumetric microscopic imaging scans |
| SegmentationParamset | All parameters for segmenting volumetric scans |
| SegmentationTask | Task defined by a combination of Volume and SegmentationParamset |
| Segmentation | Results of the segmentation |
| Segmentation.Mask | Masks identified in the segmentation procedure |

### `volume_matching` schema ([API docs](https://datajoint.com/docs/elements/element-zstack/api/element_zstack/volume))

| Table | Description |
| --- | --- |
| VolumeMatchTask | Task defining volume matching processsing task |
| VolumeMatchingTask.Volume | Segmentation data for the volume matching task |
| VolumeMatch | The core table that executes the volume matching task  |
| VolumeMatch.Transformation | Transformation matrix of the volume matching |
| VolumeMatch.CommonMask | Common mask identified in the volume matching procedure |
| VolumeMatch.VolumeMask | Volume mask identified in the volume matching procedure |

## Roadmap

Further development of this Element is community driven. Upon user requests and based on
guidance from the Scientific Steering Group we will add the following features to this
Element:

- Advanced curation of neuroglancer URLs