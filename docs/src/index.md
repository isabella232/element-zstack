# Element ZStack

DataJoint Element for z-stack (volumetric) imaging, features cell segmentation with 
[cellpose](https://github.com/MouseLand/cellpose){:target="_blank"}, data upload to 
[BossDB](https://bossdb.org/){:target="_blank"}, and data visualization with 
[Neuroglancer](https://github.com/google/neuroglancer){:target="_blank"}. DataJoint 
Elements collectively standardize and automate data collection and analysis for 
neuroscience experiments. Each Element is a modular pipeline for data storage and 
processing with corresponding database tables that can be combined with other Elements 
to assemble a fully functional pipeline.

## Experiment Flowchart

![flowchart](https://raw.githubusercontent.com/datajoint/element-zstack/main/images/flowchart.svg)

## Data Pipeline Diagram

![pipeline](https://raw.githubusercontent.com/datajoint/element-zstack/main/images/pipeline.svg)

## Example Data on BossDB

![Bossdb
Data](https://github.com/datajoint/element-zstack/blob/main/images/BossDB_screenshot.png)

## Example Visualization on Neuroglancer

![Neuroglancer Visualization](https://github.com/datajoint/element-zstack/blob/main/images/Neuroglancer_screenshot.png)

## Getting Started

+ Install from PyPI

     ```bash
     pip install element-zstack
     ```

+ [Data Pipeline](./pipeline.md) - Pipeline and table descriptions

+ [Tutorials](./tutorials/index.md) - Start building your data pipeline

+ [Code Repository](https://github.com/datajoint/element-zstack/){:target="_blank"}
