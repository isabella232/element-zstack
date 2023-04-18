import importlib
import inspect
import logging
from pathlib import Path

import numpy as np
from tifffile import TiffFile

import datajoint as dj
from element_interface.utils import dict_to_uuid, find_full_path


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
        schema_name (str): schema name on the database server to activate the `zstack` element
        create_schema (bool): when True (default), create schema in the database if it
                            does not yet exist.
        create_tables (bool): when True (default), create schema tables in the database
                             if they do not yet exist.
        linking_module (str): A string containing the module name or module containing
            the required dependencies to activate the schema.

    Dependencies:
    Tables:
        Scan: A parent table to Volume
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


def get_volume_tif_file(scan_key: dict) -> (str, Path):
    """Retrieve the full path to the TIF file of the volumetric data associated with a given scan.
    Args:
        scan_key: Primary key of a Scan entry.
    Returns:
        Full path to the TIF file of the volumetric data (Path or str).
    """
    return _linking_module.get_volume_tif_file(scan_key)


# --------------------------------------- Schema ---------------------------------------


@schema
class Volume(dj.Imported):
    definition = """
    -> Scan
    ---
    px_width: int # total number of voxels in x dimension
    px_height: int # total number of voxels in y dimension
    px_depth: int # total number of voxels in z dimension
    depth_mean_brightness=null: longblob  # mean brightness of each slice across the depth (z) dimension of the stack
    volume: longblob  # volumetric data - np.ndarray with shape (z, y, x)
    """

    def make(self, key):
        vol_tif_fp = get_volume_tif_file(key)
        volume_data = TiffFile(vol_tif_fp[0]).asarray()

        self.insert1(
            dict(
                **key,
                volume=volume_data,
                px_width=volume_data.shape[2],
                px_height=volume_data.shape[1],
                px_depth=volume_data.shape[0],
                depth_mean_brightness=volume_data.mean(axis=(1, 2)),
            )
        )


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
        params: dict,
        paramset_desc: str = "",
        paramset_idx: int = None,
    ):
        """Inserts new parameters into the table.

        Args:
            segmentation_method (str): name of the segmentation method (e.g. cellpose)
            params (dict): segmentation parameters
            paramset_desc (str, optional): description of the parameter set
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
    -> SegmentationParamset
    ---
    segmentation_output_dir='': varchar(255)  #  Output directory of the segmented results relative to root data directory
    task_mode='load': enum('load', 'trigger')
    """


@schema
class Segmentation(dj.Computed):
    definition = """
    -> SegmentationTask
    """

    class Mask(dj.Part):
        definition = """ # A mask produced by segmentation.
        -> master
        mask            : smallint
        ---
        mask_npix       : int       # number of pixels in ROIs
        mask_center_x   : float     # X component of the 3D mask centroid in pixel units
        mask_center_y   : float     # Y component of the 3D mask centroid in pixel units
        mask_center_z   : float     # Z component of the 3D mask centroid in pixel units
        mask_xpix       : longblob  # x coordinates in pixels units
        mask_ypix       : longblob  # y coordinates in pixels units
        mask_zpix       : longblob  # z coordinates in pixels units
        mask_weights    : longblob  # weights of the mask at the indices above
        """

    def make(self, key):
        # NOTE: convert seg data to unit8 instead of uint64
        task_mode, seg_method, output_dir, params = (
            SegmentationTask * SegmentationParamset & key
        ).fetch1(
            "task_mode", "segmentation_method", "segmentation_output_dir", "params"
        )
        output_dir = find_full_path(get_volume_root_data_dir(), output_dir).as_posix()
        if task_mode == "trigger" and seg_method.lower() == "cellpose":
            from cellpose import models as cellpose_models

            volume_data = (Volume & key).fetch1("volume")
            model = cellpose_models.CellposeModel(model_type=params["model_type"])
            cellpose_results = model.eval(
                [volume_data],
                diameter=params["diameter"],
                channels=params.get("channels", [[0, 0]]),
                min_size=params["min_size"],
                z_axis=0,
                do_3D=params["do_3d"],
                anisotropy=params["anisotropy"],
                progress=True,
            )
            masks, flows, styles = cellpose_results

            mask_entries = []
            for mask_id in set(masks[0].flatten()) - {0}:
                mask = np.argwhere(masks[0] == mask_id)
                mask_zpix, mask_ypix, mask_xpix = mask.T
                mask_npix = mask.shape[0]
                mask_center_z, mask_center_y, mask_center_x = mask.mean(axis=0)
                mask_weights = np.full_like(mask_zpix, 1)
                mask_entries.append(
                    {
                        **key,
                        "mask": mask_id,
                        "mask_npix": mask_npix,
                        "mask_center_x": mask_center_x,
                        "mask_center_y": mask_center_y,
                        "mask_center_z": mask_center_z,
                        "mask_xpix": mask_xpix,
                        "mask_ypix": mask_ypix,
                        "mask_zpix": mask_zpix,
                        "mask_weights": mask_weights,
                    }
                )
        else:
            raise NotImplementedError

        self.insert1(key)
        self.Mask.insert(mask_entries)
