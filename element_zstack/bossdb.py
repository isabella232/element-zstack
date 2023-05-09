import importlib
import inspect
import logging

import datajoint as dj
import numpy as np
from . import volume

from .export.bossdb_interface import BossDBUpload

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
        volume.Volume: A parent table to VolumeUploadTask
        volume.VoxelSize: A dependency of BossDBURLs
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


@schema
class VolumeUploadTask(dj.Manual):
    """Define the image and segmentation data to upload to BossDB.

    Attributes:
        volume.Volume (foreign key): Primary key from `Volume`.
        upload_type (enum): One of 'image' (volumetric image) or 'annotation'
        (segmentation data).
        collection_name (varchar(64)): Name of the collection on BossDB.
        experiment_name (varchar(64)): Name of the experiment on BossDB.
        channel_name (varchar(64)): Name of the channel on BossDB.
    """

    definition = """
    -> volume.Volume
    -> volume.Segmentation
    upload_type='image': enum('image', 'annotation')
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
        bossdb_url (varchar(512)): BossDB URL for the uploaded data.
        neuroglancer_url (varchar(1024)): Neuroglancer URL for the uploaded data.
    """

    definition = """
    -> VolumeUploadTask
    -> volume.VoxelSize
    ---
    bossdb_url: varchar(512)
    neuroglancer_url='': varchar(1024)
    """

    def get_neuroglancer_url(self, collection, experiment, channel):
        base_url = f"boss://https://api.bossdb.io/{collection}/{experiment}/{channel}"
        return (
            "https://neuroglancer.bossdb.io/#!{'layers':{'"
            + f"{experiment}"
            + "':{'source':'"
            + base_url
            + "','name':'"
            + f"{channel}"
            + "'}}}"
        )

    def make(self, key):
        """Upload data to bossdb."""

        collection, experiment, channel, upload_type = (VolumeUploadTask & key).fetch1(
            "collection_name", "experiment_name", "channel_name", "upload_type"
        )

        voxel_width, voxel_height, voxel_depth = (volume.VoxelSize & key).fetch1(
            "width", "height", "depth"
        )

        if upload_type == "image":
            data = (volume.Volume & key).fetch1("volume")
            neuroglancer_url = self.get_neuroglancer_url(
                collection, experiment, channel
            )

        elif upload_type == "annotation":
            neuroglancer_url = self.get_neuroglancer_url(
                collection, experiment, channel
            )
            z_size, y_size, x_size = (volume.Volume & key).fetch1(
                "px_depth", "px_height", "px_width"
            )
            data = np.zeros((z_size, y_size, x_size))

            mask_ids, x_mask_pix, y_mask_pix, z_mask_pix = (
                volume.Segmentation.Mask & key
            ).fetch("mask", "mask_xpix", "mask_ypix", "mask_zpix")

            for idx, mask in enumerate(mask_ids):
                data[np.s_[z_mask_pix[idx], y_mask_pix[idx], x_mask_pix[idx]]] = mask
            data = data.astype("uint64")

        boss_url = f"bossdb://{collection}/{experiment}/{channel}"
        BossDBUpload(
            url=boss_url,
            volume_data=data,
            data_description=upload_type,
            voxel_size=(voxel_depth, voxel_height, voxel_width),
            voxel_units="millimeters",
        ).upload()

        self.insert1(
            dict(
                key,
                bossdb_url=boss_url,
                neuroglancer_url=neuroglancer_url
                if neuroglancer_url is not None
                else "null",
            )
        )
