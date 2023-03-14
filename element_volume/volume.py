import importlib
import inspect
import logging
from pathlib import Path
from typing import Optional

import datajoint as dj
import numpy as np
from element_interface.utils import dict_to_uuid, find_full_path
from numpy.typing import DTypeLike

from .export.bossdb import BossDBUpload
from .readers.bossdb import BossDBInterface

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
        get_volume_tif_files: When given a scan key (dict), returns the list of TIFF files 
            associated with a given Volume.
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
    """Fetches absolute data path to ephys data directories.

    The absolute path here is used as a reference for all downstream relative paths used in DataJoint.

    Returns:
        A list of the absolute path(s) to ephys data directories.
    """
    root_directories = _linking_module.get_vol_root_data_dir()
    if isinstance(root_directories, (str, Path)):
        root_directories = [root_directories]

    return root_directories


def get_volume_tif_files(scan_key: dict) -> list:
    """Retrieve the list of TIFF files associated with a given Volume.
    Args:
        scan_key: Primary key of a Scan entry.
    Returns:
        A list of TIFF files' full file-paths.
    """
    return _linking_module.get_volume_tif_files(scan_key)


# --------------------------------------- Schema ---------------------------------------

@schema
class Volume(dj.Imported):
    definition = """
    -> Scan
    -> Channel
    ---
    x_size: int # total number of voxels in x dimension
    y_size: int # total number of voxels in y dimension
    z_size: int # total number of voxels in z dimension
    volume: longblob  # volumetric data - np.ndarray with shape (x, y, z)
    depth_mean_brightness=null: longblob  # mean brightness of each slice across the depth (z) dimension of the stack
    """


@schema
class SegmentationParamset(dj.Lookup):
    definition = """
    paramset_idx: int
    ---
    segmentation_method: varchar(32)
    paramset_desc="": varchar(256)
    params: longblob
    paramset_hash: uuid
    unique index (paramset_hash)
    """

    @classmethod
    def insert_new_params(
        cls,
        segmentation_method: str,
        paramset_desc: str = "",
        params: dict = {},
        paramset_idx: int = None,
    ):
        """Inserts new parameters into the table.

        Args:
            segmentation_method (str): name of the clustering method.
            paramset_desc (str): description of the parameter set
            params (dict): clustering parameters
            paramset_idx (int, optional): Unique parameter set ID. Defaults to None.
        """
        if paramset_idx is None:
            paramset_idx = (
                dj.U().aggr(cls, n="max(paramset_idx)").fetch1("n") or 0
            ) + 1

        param_dict = {
            "segmentation_method": segmentation_method,
            "paramset_desc": paramset_desc,
            "params": params,
            "paramset_idx": paramset_idx,
            "paramset_hash": dict_to_uuid(
                {**params, "segmentation_method": segmentation_method}
            ),
        }
        param_query = cls & {"paramset_hash": param_dict["paramset_hash"]}

        if param_query:  # If the specified param-set already exists
            existing_paramset_idx = param_query.fetch1("paramset_idx")
            if (
                existing_paramset_idx == paramset_idx
            ):  # If the existing set has the same paramset_idx: job done
                return
            else:  # If not same name: human error, trying to add the same paramset with different name
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
            cls.insert1(param_dict)


@schema
class SegmentationTask(dj.Manual):
    definition = """
    -> Volume
    ---
    task_mode='load': enum('load', 'trigger')
    -> [nullable] SegmentationParamset
    -> [nullable] URLs.Segmentation
    """


@schema
class Segmentation(dj.Imported):
    definition = """
    -> SegmentationTask
    ---
    segmentation_data=null: longblob
    """

    class Cell(dj.Part):
        definition = """
        -> master
        cell_id : int
        """

    def make(self, key):
        # NOTE: convert seg data to unit8 instead of uint64
        (task_mode, seg_method, resolution_id, url, params) = (
            SegmentationTask * SegmentationParamset * Resolution & key
        ).fetch1(
            "task_mode",
            "segmentation_method",
            "downsampling",
            "url",
            "params",
        )
        if task_mode == "trigger" or seg_method.lower() != "bossdb":
            raise NotImplementedError
        else:
            self.download(url=url, downsampling=resolution_id, **params)

    @classmethod
    def download(
        cls,
        url: Optional[str],
        downsampling: Optional[int] = 0,
        session_key: Optional[dict] = None,
        **kwargs,
    ):
        data = BossDBInterface(url, resolution=downsampling, session_key=session_key)
        data.load_data_into_element(table="Segmentation", **kwargs)


@schema
class CellMapping(dj.Computed):  # TODO: FIX cell table foreign key ref
    definition = """
    -> Segmentation.Cell
    -> Mask
    """

    def make(self, key):
        raise NotImplementedError


@schema
class ConnectomeParamset(dj.Lookup):
    definition = """
    paramset_idx: int
    ---
    connectome_method: varchar(32)
    paramset_desc="": varchar(256)
    params: longblob
    paramset_hash: uuid
    unique index (paramset_hash)
    """

    @classmethod
    def insert_new_params(
        cls,
        connectome_method: str,
        paramset_desc: str,
        params: dict,
        paramset_idx: int = None,
    ):
        """Inserts new parameters into the table.

        Args:
            connectome_method (str): name of the clustering method.
            paramset_desc (str): description of the parameter set
            params (dict): clustering parameters
            paramset_idx (int, optional): Unique parameter set ID. Defaults to None.
        """
        if paramset_idx is None:
            paramset_idx = (
                dj.U().aggr(cls, n="max(paramset_idx)").fetch1("n") or 0
            ) + 1

        param_dict = {
            "connectome_method": connectome_method,
            "paramset_desc": paramset_desc,
            "params": params,
            "paramset_idx": paramset_idx,
            "paramset_hash": dict_to_uuid(
                {**params, "connectome_method": connectome_method}
            ),
        }
        param_query = cls & {"paramset_hash": param_dict["paramset_hash"]}

        if param_query:  # If the specified param-set already exists
            existing_paramset_idx = param_query.fetch1("paramset_idx")
            if (
                existing_paramset_idx == paramset_idx
            ):  # If the existing set has the same paramset_idx: job done
                return
            else:  # If not same name: human error, trying to add the same paramset with different name
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
            cls.insert1(param_dict)


@schema
class ConnectomeTask(dj.Manual):
    definition = """
    -> Segmentation
    -> ConnectomeParamset
    ---
    task_mode='load': enum('load', 'trigger')
    -> [nullable] URLs.Connectome
    """


@schema
class Connectome(dj.Imported):
    definition = """
    -> ConnectomeTask
    """

    class Connection(dj.Part):
        definition = """
        -> Segmentation.Cell.proj(pre_synaptic='cell_id')
        -> Segmentation.Cell.proj(post_synaptic='cell_id')
        ---
        connectivity_strength: float # TODO: rename based on existing standards
        """

    def make(self, key):
        raise NotImplementedError
