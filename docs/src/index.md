# Element ZStack

DataJoint Element for volumetric data segmentation with Cellpose and cell matching across sessions. DataJoint Elements
collectively standardize and automate data collection and analysis for neuroscience
experiments. Each Element is a modular pipeline for data storage and processing with
corresponding database tables that can be combined with other Elements to assemble a
fully functional pipeline. This Element is comprised of the `volume` and
`volume_matching` schemas. 

- `volume`: features a DataJoint pipeline design for volumetric neuroimaging data, including
  segmentation and connectomics.

- `volume_matching`: features a DataJoint pipeline design for cell matching
  across sessions.

Visit the [Concepts page](./concepts.md) for more information about the use
cases of the schemas and an explanantion of the tables. To get started with
building your own data pipeline, visit the [Tutorials](./tutorials/index.md) page.