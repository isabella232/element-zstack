import importlib
import inspect
import logging
from pathlib import Path

import numpy as np
from tifffile import TiffFile

import datajoint as dj
from element_interface.utils import dict_to_uuid, find_full_path, find_root_directory


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
        create_schema (bool): when True (default), create schema in the database
        if it does not yet exist.
        create_tables (bool): when True (default), create schema tables in the database if they do not yet exist.
        linking_module (str): A string containing the module name or module
        containing the required dependencies to activate the schema.

    Tables:
        Scan: A parent table to Volume
    Functions:
        get_volume_root_data_dir: Returns absolute path for root data
        director(y/ies) with all volumetric data, as a list of string(s).
        get_volume_tif_file: When given a scan key (dict), returns the full path
        to the TIF file of the volumetric data associated with a given scan.
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
    """Details about the volumetric microscopic imaging scans.

    Attributes:
        Scan (foreign key): Primary key from `imaging.Scan`.
        px_width (int): total number of voxels in the x dimension.
        px_height (int): total number of voxels in the y dimension.
        px_depth (int): total number of voxels in the z dimension.
        depth_mean_brightness (longblob): optional, mean brightness of each slice across
        the depth (z) dimension of the stack.
        volume_file_path (str): Relative path of the volumetric data with shape (z, y, x)
    """

    definition = """
    -> Scan
    ---
    px_width: int # total number of voxels in x dimension
    px_height: int # total number of voxels in y dimension
    px_depth: int # total number of voxels in z dimension
    depth_mean_brightness=null: longblob  # mean brightness of each slice across the depth (z) dimension of the stack
    volume_file_path: varchar(255)  # Relative path of the volumetric data with shape (z, y, x)
    """

    def make(self, key):
        """Populate the Volume table with volumetric microscopic imaging data."""
        volume_file_path = get_volume_tif_file(key)
        volume_data = TiffFile(volume_file_path).asarray()

        root_dir = find_root_directory(get_volume_root_data_dir(), volume_file_path)
        volume_relative_path = Path(volume_file_path).relative_to(root_dir).as_posix()

        self.insert1(
            dict(
                **key,
                volume_file_path=volume_relative_path,
                px_width=volume_data.shape[2],
                px_height=volume_data.shape[1],
                px_depth=volume_data.shape[0],
                depth_mean_brightness=volume_data.mean(axis=(1, 2)),
            )
        )


@schema
class VoxelSize(dj.Manual):
    """Voxel size information about a volume in millimeters.

    Attributes:
        Volume (foreign key): Primary key from `Volume`.
        width (float): Voxel size in mm in the x dimension.
        height (float): Voxel size in mm in the y dimension.
        depth (float): Voxel size in mm in the z dimension.
    """

    definition = """
    -> Volume
    ---
    width: float # voxel size in mm in the x dimension
    height: float # voxel size in mm in the y dimension
    depth: float # voxel size in mm in the z dimension
    """


@schema
class SegmentationParamSet(dj.Lookup):
    """Parameter set used for segmentation of the volumetric microscopic imaging
    scan.

    Attributes:
        paramset_idx (int): Unique parameter set identifier.
        segmentation_method (str): Name of the segmentation method (e.g.
        cellpose).
        paramset_desc (str): Optional. Parameter set description.
        params (longblob): Parameter set. Dictionary of all applicable
        parameters for the segmentation method.
        paramset_hash (uuid): A universally unique identifier for the parameter set.
    """

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
    """Defines the method and parameter set which will be used to segment a volume in the downstream `Segmentation` table.  This table currently supports triggering segmentation with `cellpose`.

    Attributes:
        Volume (foreign key): Primary key from `Volume`.
        SegmentationParamSet (foreign key): Primary key from
        `SegmentationParamSet`.
        segmentation_output_dir (str): Optional. Output directory of the
        segmented results relative to the root data directory.
        task_mode (enum): `Trigger` computes segmentation or `load` imports existing results.
    """

    definition = """
    -> Volume
    -> SegmentationParamSet
    ---
    segmentation_output_dir='': varchar(255)  #  Output directory of the segmented results relative to root data directory
    task_mode='load': enum('load', 'trigger')
    """


@schema
class Segmentation(dj.Computed):
    """Performs segmentation on the volume (and with the method and parameter set) defined in the `SegmentationTask` table.

    Attributes:
        SegmentationTask (foreign key): Primary key from `SegmentationTask`.
    """

    definition = """
    -> SegmentationTask
    """

    class Mask(dj.Part):
        """Details of the masks identified from the segmentation.

        Attributes:
            Segmentation (foreign key): Primary key from `Segmentation`.
            mask (int): Unique mask identifier.
            mask_npix (int): Number of pixels in the mask.
            mask_center_x (float): Center x coordinate in pixels.
            mask_center_y (float): Center y coordinate in pixels.
            mask_center_z (float): Center z coordinate in pixels.
            mask_xpix (longblob): X coordinates in pixels.
            mask_ypix (longblob): Y coordinates in pixels.
            mask_zpix (longblob): Z coordinates in pixels.
            mask_weights (longblob): Weights of the mask at the indices above.
        """

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
        """Populate the Segmentation and Segmentation.Mask tables with results of cellpose segmentation."""

        task_mode, seg_method, output_dir, params = (
            SegmentationTask * SegmentationParamSet & key
        ).fetch1(
            "task_mode", "segmentation_method", "segmentation_output_dir", "params"
        )
        output_dir = find_full_path(get_volume_root_data_dir(), output_dir).as_posix()
        if task_mode == "trigger" and seg_method.lower() == "cellpose":
            from cellpose import models as cellpose_models

            volume_relative_path = (Volume & key).fetch1("volume_file_path")
            volume_file_path = find_full_path(
                get_volume_root_data_dir(), volume_relative_path
            ).as_posix()
            volume_data = TiffFile(volume_file_path).asarray()

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
