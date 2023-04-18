import importlib
import inspect
import logging
from pathlib import Path

import numpy as np
from tifffile import TiffFile

import datajoint as dj
from element_interface.utils import dict_to_uuid, find_full_path
from element_zstack.volume import get_volume_root_data_dir, get_volume_tif_file

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
class UploadParamSet(dj.Lookup):
    definition = """
    paramset_idx: smallint
    ---
    paramset_desc: varchar(512)
    param_set_hash: uuid
    params: longblob
    """

    @classmethod
    def insert_new_params(cls, paramset_idx: int, paramset_desc: str, params: dict):
        params_dict = {
            "paramset_idx": paramset_idx,
            "paramset_desc": paramset_desc,
            "params": params,
            "param_set_hash": dict_to_uuid(params),
        }
        param_query = cls & {"param_set_hash": params_dict["param_set_hash"]}

        if param_query:
            existing_paramset_idx = param_query.fetch1("paramset_idx")
            if existing_paramset_idx == paramset_idx:
                return
            else:
                raise dj.DataJointError(
                    f"The specified param-set already exists"
                    f" - with paramset_idx: {existing_paramset_idx}"
                )
        else:
            if {"paramset_idx": paramset_idx} in cls.proj():
                raise dj.DataJointError(
                    f"The specified paramset_idx {paramset_idx} already exists,"
                    f" please pick a different one."
                )
            cls.insert1(params_dict)


@schema
class VolumeUploadTask(dj.Manual):
    definition = """
    -> Volume
    -> UploadParamSet
    upload_type='image': enum('image', 'annotation')
    ---
    collection_name: varchar(64)
    experiment_name: varchar(64)
    channel_name: varchar(64)
    """


@schema
class BossDBURLs(dj.Imported):
    definition = """
    -> VolumeUploadTask
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
        upload_params = (UploadParamSet & key).fetch1("params")
        collection, experiment, channel, upload_type = (VolumeUploadTask & key).fetch1(
            "collection_name", "experiment_name", "channel_name", "upload_type"
        )

        if upload_type == "image":
            data_file = get_volume_tif_file(key)
            ng_url = self.get_neuroglancer_url(collection, experiment, channel)

        elif upload_type == "annotation":
            from ..volume import SegmentationTask

            ng_url = None
            output_dir = (SegmentationTask & key).fetch1("segmentation_output_dir")
            if output_dir == "":
                data_file = sorted(get_volume_root_data_dir()[0].glob("*seg*.npy"))
            else:
                data_file = sorted(
                    find_full_path(get_volume_root_data_dir(), output_dir)
                    .as_posix()
                    .glob("*seg*.npy")
                )

        boss_url = f"bossdb://{collection}/{experiment}/{channel}"
        voxel_size = upload_params.get("voxel_size")
        voxel_units = upload_params.get("voxel_units")
        BossDBUpload(
            url=boss_url,
            data_dir=data_file,
            data_description=upload_type,
            voxel_size=voxel_size,
            voxel_units=voxel_units,
        ).upload()

        self.insert1(
            dict(
                key,
                bossdb_url=boss_url,
                neuroglancer_url=ng_url if ng_url is not None else "null",
            )
        )
