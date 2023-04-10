# Element ZStack

This element features DataJoint schemas for processing volumetric data and
uploading to the Brain Observatory Storage Service and Database (BossDB). Each Element is a modular pipeline for data storage and processing with
corresponding database tables that can be combined with other Elements to assemble a
fully functional pipeline. This Element is comprised of the `volume` and
`volume_matching` schemas. 

- `volume`: features a DataJoint pipeline design for volumetric neuroimaging data, including
  segmentation and connectomics.

- `volume_matching`: ....

Visit the [Concepts page](./concepts.md) for more information about the use
cases of `volume` schemas and an explanantion of the tbales. To get started with
building your own data pipeline, visit the Tutorials page.

- is not a complete workflow by itself, but rather a modular design of
  tables and dependencies.

- can be flexibly attached to any DataJoint workflow.
