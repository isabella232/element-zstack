"""
Optional schema to provide URLs to the main volume schema.
"""
import logging
import datajoint as dj

from . import volume


schema = dj.Schema()

logger = logging.getLogger("datajoint")


def activate(
    schema_name: str,
    *,
    create_schema: bool = True,
    create_tables: bool = True,
):
    """Activate this schema

    Args:
        schema_name (str): schema name on the database server to use for activation
        create_schema (bool): when True (default), create schema in the database if it
            does not yet exist.
        create_tables (bool): when True (default), create schema tables in the database
            if they do not yet exist.
    """

    schema.activate(
        schema_name,
        create_schema=create_schema,
        create_tables=create_tables,
    )


@schema
class Resolution(dj.Lookup):
    definition = """ # Resolution of stored data
    resolution_id: varchar(32) # Shorthand for convention
    ---
    voxel_unit: varchar(16) # e.g., nanometers
    voxel_z_size: float # size of one z dimension voxel in voxel_units
    voxel_y_size: float # size of one y dimension voxel in voxel_units
    voxel_x_size: float # size of one x dimension voxel in voxel_units
    downsampling=0: int # Downsampling iterations relative to raw data
    """


@schema
class BossDBVolume(dj.Manual):
    definition = """ # Dataset of a contiguous volume
    -> volume.Volume
    -> Resolution
    ---
    z_size: int # total number of voxels in z dimension
    y_size: int # total number of voxels in y dimension
    x_size: int # total number of voxels in x dimension
    slicing_dimension='z': enum('x','y','z') # perspective of slices
    channel: varchar(64) # data type or modality
    """

    @classmethod
    def download(
        cls,
        url: str,
        downsampling: Optional[int] = 0,
        session_key: Optional[dict] = None,
        **kwargs,
    ):
        data = BossDBInterface(url, resolution=downsampling, session_key=session_key)
        data.insert_channel_as_url(data_channel="Volume")
        data.load_data_into_element(**kwargs)

    @classmethod
    def return_bossdb_data(self, volume_key: dict):
        url, res_id = (Volume & volume_key).fetch1("url", "resolution_id")
        downsampling = (Resolution & dict(resolution_id=res_id)).fetch("downsampling")
        return BossDBInterface(url, resolution=downsampling)

    @classmethod
    def upload(
        cls,
        volume_key: dict,
        session_key: Optional[dict] = None,
        upload_from: Optional[str] = "table",
        data_dir: Optional[str] = None,
        dtype: Optional[DTypeLike] = None,
        **kwargs,
    ):
        # NOTE: uploading from data_dir (local rel path) assumes 1 image per z slice
        # If not upload_from 'table', upload files in data_dir

        if upload_from == "table":
            data = (Volume & volume_key).fetch1("volume_data")
            dtype = dtype or data.dtype  # if provided, fetch from
        else:  # Uploading from image files
            data = None

            if not dtype:
                raise ValueError("Must specify dtype when loading data from images")

            if not data_dir and session_key:
                data_dir = find_full_path(
                    get_vol_root_data_dir(),
                    get_session_directory(session_key),
                )
            if not Path(data_dir).is_absolute():
                raise ValueError(f"Could not find absolute path to data: {data_dir}")

        if isinstance(dtype, str):
            dtype = np.dtype(dtype)
        if dtype and dtype not in [np.dtype("uint8"), np.dtype("uint16")]:
            raise ValueError("BossDB only accepts uint8 or uint16 image data.")

        (
            url,
            downsampling,
            z_size,
            y_size,
            x_size,
            voxel_z_size,
            voxel_y_size,
            voxel_x_size,
            voxel_unit,
        ) = (Volume * Resolution & volume_key).fetch1(
            "url",
            "downsampling",
            "z_size",
            "y_size",
            "x_size",
            "voxel_z_size",
            "voxel_y_size",
            "voxel_x_size",
            "voxel_unit",
        )

        bossdb = BossDBUpload(
            url=url,
            raw_data=data,
            data_dir=data_dir,
            voxel_size=(float(i) for i in (voxel_z_size, voxel_y_size, voxel_x_size)),
            voxel_units=voxel_unit,
            shape_zyx=(int(i) for i in (z_size, y_size, x_size)),
            resolution=downsampling,
            dtype=dtype,
            **kwargs,
        )
        bossdb.upload()


@schema
class BossDBCollection(dj.Manual):
    definition = """
    bossdb_collection: varchar(64)
    """


@schema
class BossDBExperiment(dj.Manual):
    definition = """
    -> BossDBCollection
    bossdb_experiment: varchar(64)
    """


@schema
class BossDBURL(dj.Lookup):
    definition = """
    -> BossDBExperiment
    -> BossDBVolume
    """

    class Volume(dj.Part):
        definition = """
        -> master
        ---
        url: varchar(64)
        """

    class Segmentation(dj.Part):
        definition = """
        -> master
        ---
        url: varchar(64)
        """

    class Connectome(dj.Part):
        definition = """
        ---
        url: varchar(64)
        """

    @classmethod
    def load_bossdb_info(
        cls,
        collection: str,
        experiment: str,
        volume: str,
        segmentation: str = "",
        connectome: str = "",
        skip_duplicates: bool = False,
        test_exists: bool = False,  # Run a check to see if the data already exists
    ):

        from .readers.bossdb import BossDBInterface  # isort: skip

        collection_experiment = f"{collection}/{experiment}"
        master_key = dict(collection_experiment=collection_experiment)
        base_url = f"bossdb://{collection_experiment}/"
        vol_url = base_url + volume
        seg_url = base_url + segmentation
        con_url = base_url + connectome

        if test_exists:
            for url in [vol_url, seg_url, con_url]:
                if url != base_url and not BossDBInterface(url).exists:
                    logger.warning(
                        f"The following BossDB url does not yet exist: {url}"
                    )

        with cls.connection.transaction:
            cls.insert1(master_key, skip_duplicates=skip_duplicates)

            cls.Volume.insert1(
                {**master_key, "url": vol_url},
                skip_duplicates=skip_duplicates,
            )
            if segmentation:
                cls.Segmentation.insert1(
                    {**master_key, "url": seg_url},
                    skip_duplicates=skip_duplicates,
                )
            if connectome:
                cls.Connectome.insert1(
                    {**master_key, "url": con_url},
                    skip_duplicates=skip_duplicates,
                )

    @classmethod
    def get_neuroglancer_url(cls, key, table="Volume"):
        url = (getattr(cls, table) & key).fetch1("url")

        return (
            "https://neuroglancer.bossdb.io/#!{'layers':{'image':{'source':'"
            + url
            + "'}}}"
        )
