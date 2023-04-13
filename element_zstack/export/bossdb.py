import importlib
import inspect
import logging
from pathlib import Path

import numpy as np
from tifffile import TiffFile

import datajoint as dj
from element_interface.utils import dict_to_uuid, find_full_path

from .upload_utils import BossDBUpload
from ..volume import Volume

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
        schema_name (str): schema name on the database server to activate the `lab` element
        create_schema (bool): when True (default), create schema in the database if it
                            does not yet exist.
        create_tables (bool): when True (default), create schema tables in the database
                             if they do not yet exist.
        linking_module (str): A string containing the module name or module containing
            the required dependencies to activate the schema.

    Dependencies:
    Tables:
        Scan: A parent table to Volume
        Channel: A parent table to Volume
    Functions:
        get_volume_root_data_dir: Returns absolute path for root data director(y/ies) with
            all volumetric data, as a list of string(s).
        get_volume_tif_file: When given a scan key (dict), returns the full path to the
            TIF file of the volumetric data associated with a given scan.
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
    definition = """
    -> Volume
    ---
    collection_name: varchar(64)
    experiment_name: varchar(64)
    channel_name: varchar(64)
    upload_type='image': enum('image', 'annotation')
    neuroglancer_link=null: boolean
    """


@schema
class BossDBURLs(dj.Imported):
    definition = """
    -> VolumeUploadTask
    ---
    bossdb_url: varchar(512)
    neuroglancer_url='': varchar(1024)
    """

    @property
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
        from element_volume.volume import get_volume_root_data_dir, get_volume_tif_file

        upload_type = (VolumeUploadTask & key).fetch1("upload_type")
        if upload_type == "image":
            collection, experiment, channel, upload_type = (
                VolumeUploadTask & key
            ).fetch1(
                "collection_name", "experiment_name", "channel_name", "upload_type"
            )

            volume_file = get_volume_tif_file(key)
            boss_url = f"bossdb://{collection}/{experiment}/{channel}"
            voxel_size = [1, 1, 1]
            voxel_units = "nanometers"
            BossDBUpload(
                url=boss_url,
                data_dir=volume_file,
                data_description=upload_type,
                voxel_size=voxel_size,
                voxel_units=voxel_units,
            )
