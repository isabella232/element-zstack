import importlib
import inspect
import logging

import datajoint as dj
from element_interface.utils import dict_to_uuid, find_full_path
from element_zstack import volume
from element_zstack.volume import get_volume_root_data_dir, get_volume_tif_file

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
    """A pairing of Volume data to be uploaded and parameter set to be used for
    the upload.

    Attributes:
        volume.Volume (foreign key): Primary key from `Volume`.
        upload_type (str): One of 'image' (volumetric image) or 'annotation'
        (segmentation data).
        collection_name (varchar(64)): Name of the collection on bossdb.
        experiment_name (varchar(64)): Name of the experiment on bossdb.
        channel_name (varchar(64)): Name of the channel on bossdb.
    """

    definition = """
    -> volume.Volume
    upload_type='image': enum('image', 'annotation')
    ---
    collection_name: varchar(64)
    experiment_name: varchar(64)
    channel_name: varchar(64)
    """


@schema
class BossDBURLs(dj.Imported):
    """Uploads data to bossdb and stores the bossdb and neuroglancer URLs.

    Attributes:
        VolumeUploadTask (foreign key): Primary key from `VolumeUploadTask`.
        bossdb_url (varchar(512)): bossdb URL for the uploaded data.
        neuroglancer_url (varchar(1024)): neuroglancer URL for the uploaded data.
    """

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

    @property
    def key_source(self):
        """Limit the upload to entries that have voxel sizes defined in the database."""
        return volume.Volume & volume.VoxelSize

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
            ng_url = self.get_neuroglancer_url(collection, experiment, channel)

        elif upload_type == "annotation":
            ng_url = None
            data = (volume.Segmentation.Mask & key).fetch()

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
                neuroglancer_url=ng_url if ng_url is not None else "null",
            )
        )
