import importlib
import inspect
import logging
from pathlib import Path

import datajoint as dj
import numpy as np
from tifffile import TiffFile

from . import volume

from .export.bossdb_interface import BossDBUpload

from element_interface.utils import find_full_path

logger = logging.getLogger("datajoint")

schema = dj.Schema()
_linking_module = None


def activate(
    schema_name: str,
    *,
    create_schema: bool = True,
    create_tables: bool = True,
    linking_module: str = None,
):
    """Activate this schema

    Args:
        schema_name (str): schema name on the database server to activate the `bossdb` schema
        create_schema (bool): when True (default), create schema in the database if it
                            does not yet exist.
        create_tables (bool): when True (default), create schema tables in the database
                             if they do not yet exist.
        linking_module (str): A string containing the module name or module containing
            the required dependencies to activate the schema.

    Dependencies:
    Tables:
        volume.VolumeSegmentation: A parent table to VolumeUploadTask
        volume.VoxelSize: A dependency of VolumeUpload
    Functions:
        get_volume_root_data_dir: Returns absolute path for root data
        director(y/ies) with all volumetric data, as a list of string(s).
    """

    if isinstance(linking_module, str):
        linking_module = importlib.import_module(linking_module)
    assert inspect.ismodule(
        linking_module
    ), "The argument 'linking_module' must be a module's name or a module"

    global _linking_module
    _linking_module = linking_module

    schema.activate(
        schema_name,
        create_schema=create_schema,
        create_tables=create_tables,
        add_objects=_linking_module.__dict__,
    )


# -------------------------- Functions required by the Element -------------------------


def get_volume_root_data_dir() -> list:
    """Fetches absolute data path to volume data directories.

    The absolute path here is used as a reference for all downstream relative paths used in DataJoint.

    Returns:
        A list of the absolute path(s) to volume data directories.
    """
    root_directories = _linking_module.get_volume_root_data_dir()
    if isinstance(root_directories, (str, Path)):
        root_directories = [root_directories]

    return root_directories


# --------------------------------------- Schema ---------------------------------------


@schema
class VolumeUploadTask(dj.Manual):
    """Define the image and segmentation data to upload to BossDB.

    Attributes:
        volume.Segmentation (foreign key): Primary key from `volume.Segmentation`.
        collection_name (str): Name of the collection on BossDB.
        experiment_name (str): Name of the experiment on BossDB.
        channel_name (str): Name of the channel on BossDB.
    """

    definition = """
    -> volume.Segmentation
    ---
    collection_name: varchar(64)
    experiment_name: varchar(64)
    channel_name: varchar(64)
    """


@schema
class VolumeUpload(dj.Computed):
    """Upload image and segmentation data to BossDB, and store the BossDB and Neuroglancer URLs.

    Attributes:
        VolumeUploadTask (foreign key): Primary key from `VolumeUploadTask`.
        volume.VoxelSize (foreign key): Primary key from `volume.VoxelSize`.
    """

    definition = """
    -> VolumeUploadTask
    ---
    -> volume.VoxelSize
    """

    class WebAddress(dj.Part):
        """
        Attributes:
            VolumeUpload (foreign key): Primary key from `VolumeUpload`.
            upload_type (enum): 'image' (volumetric image), 'annotation' (segmentation data), or 'image+annotation' (segmentation overlayed on image).
            web_address_type (enum): 'bossdb' or 'neuroglancer'.
            web_address (str): URL for the data or visualization website.
        """

        definition = """
        -> master
        upload_type='image': enum('image', 'annotation', 'image+annotation')
        web_address_type='bossdb': enum('bossdb', 'neuroglancer')
        ---
        web_address: varchar(2048)
        """

    def get_neuroglancer_url(self, upload_type, collection, experiment, channel):
        base_url = f"boss://https://api.bossdb.io/{collection}/{experiment}/{channel}"
        if upload_type == "image":
            return "https://neuroglancer.bossdb.io/#!" + str(
                {
                    "layers": {
                        "type": "image",
                        "source": base_url,
                        "tab": "source",
                        "name": f"{experiment}",
                    }
                }
            )
        elif upload_type == "annotation":
            return "https://neuroglancer.bossdb.io/#!" + str(
                {
                    "layers": {
                        "type": "segmentation",
                        "source": base_url + "-seg",
                        "tab": "annotations",
                        "name": f"{channel}-seg",
                    }
                }
            )
        elif upload_type == "image+annotation":
            return "https://neuroglancer.bossdb.io/#!" + str(
                {
                    "layers": [
                        {
                            "type": "image",
                            "source": base_url,
                            "tab": "source",
                            "name": f"{experiment}",
                        },
                        {
                            "type": "segmentation",
                            "source": base_url + "-seg",
                            "tab": "annotations",
                            "name": f"{channel}-seg",
                        },
                    ]
                }
            )

    def make(self, key):
        """Upload data to bossdb."""

        collection, experiment, channel = (VolumeUploadTask & key).fetch1(
            "collection_name", "experiment_name", "channel_name"
        )

        voxel_width, voxel_height, voxel_depth = (volume.VoxelSize & key).fetch1(
            "width", "height", "depth"
        )

        description = ["image", "annotation", "image+annotation"]
        full_data = []
        boss_url = []
        neuroglancer_url = []

        volume_relative_path = (volume.Volume & key).fetch1("volume_file_path")
        volume_file_path = find_full_path(
            get_volume_root_data_dir(), volume_relative_path
        ).as_posix()
        volume_data = TiffFile(volume_file_path).asarray()

        full_data.append(volume_data)
        boss_url.append(f"bossdb://{collection}/{experiment}/{channel}")

        z_size, y_size, x_size = (volume.Volume & key).fetch1(
            "px_depth", "px_height", "px_width"
        )
        segmentation_data = np.zeros((z_size, y_size, x_size))

        mask_ids, x_mask_pix, y_mask_pix, z_mask_pix = (
            volume.Segmentation.Mask & key
        ).fetch("mask", "mask_xpix", "mask_ypix", "mask_zpix")

        for idx, mask in enumerate(mask_ids):
            segmentation_data[
                np.s_[z_mask_pix[idx], y_mask_pix[idx], x_mask_pix[idx]]
            ] = mask
        full_data.append(segmentation_data.astype("uint64"))

        boss_url.append(f"bossdb://{collection}/{experiment}/{channel}-seg")

        for url, data, desc in zip(boss_url, full_data, description[:2]):
            BossDBUpload(
                url=url,
                volume_data=data,
                data_description=desc,
                voxel_size=(voxel_depth, voxel_height, voxel_width),
                voxel_units="millimeters",
            ).upload()

        self.insert1(key)
        self.WebAddress.insert(
            [
                dict(
                    key,
                    upload_type=desc,
                    web_address_type="bossdb",
                    web_address=db_url,
                )
                for desc, db_url in list(zip(description[:2], boss_url))
            ]
        )

        for desc in description:
            neuroglancer_url.append(
                self.get_neuroglancer_url(desc, collection, experiment, channel)
            )
        self.WebAddress.insert(
            [
                dict(
                    key,
                    upload_type=desc,
                    web_address_type="neuroglancer",
                    web_address=url,
                )
                for desc, url in list(zip(description, neuroglancer_url))
            ]
        )
