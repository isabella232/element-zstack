import datajoint as dj
import numpy as np
import hashlib
import uuid

from element_interface.utils import dict_to_uuid

from . import volume


schema = dj.Schema()


def activate(
    schema_name: str, *, create_schema: bool = True, create_tables: bool = True
):
    """Activate this schema.
    Args:
        schema_name (str): Schema name on the database server to activate the
            `volume_matching` schema
        create_schema (bool): When True (default), create schema in the database if it
            does not yet exist.
        create_tables (bool): When True (default), create tables in the database if they
            do not yet exist.
    """
    assert volume.schema.is_activated(), 'The "volume" schema must be activated'
    schema.activate(
        schema_name,
        create_schema=create_schema,
        create_tables=create_tables,
        add_objects=volume.__dict__,
    )


@schema
class VolumeMatchTask(dj.Manual):
    """Table defining a volume matching task. The task defined here
    is then run in the downstream table `VolumeMatch`.

    Attributes:
        volume_match_task (uuid): UUID hash for the volume matching task
    """

    definition = """  # 
    volume_match_task: uuid
    """

    class Volume(dj.Part):
        """Table defining a volumetric image to be matched in the downsteam `VolumeMatch` table.

        Attributes:
            VolumeMatchTask (foreign key): Primary key from `VolumeMatchTask`.
            volume.Segmentation (foreign key): Primary key from `volume.Segmentation`.
        """

        definition = """
        -> master
        -> volume.Segmentation
        """

    @classmethod
    def insert1(cls, vol_seg_keys, **kwargs):
        """Insert an entry for volume matching task.

        Args:
            vol_seg_keys (tuple): a tuple of two cell-segmented volumes
        """
        assert (
            len(vol_seg_keys) == 2
        ), f"Volume match task only supports matching two cell-segmented volumes, {len(vol_seg_keys)} are provided"
        vol_seg_keys = [(volume.Segmentation & k).fetch1("KEY") for k in vol_seg_keys]
        assert (
            len(set(vol_seg_keys)) == 2
        ), "The two specified cell-segmented volumes are identical"

        hashed = hashlib.md5()
        [
            hashed.update(str(k).encode())
            for k in sorted([dict_to_uuid(k) for k in vol_seg_keys])
        ]

        mkey = {"volume_match_task": uuid.UUID(hex=hashed.hexdigest())}
        if cls & mkey:
            assert len(cls.Volume & mkey & vol_seg_keys) == 2
            return

        with cls.connection.transaction:
            super().insert1(cls(), mkey, **kwargs)
            cls.Volume.insert({**mkey, **k} for k in vol_seg_keys)


@schema
class VolumeMatch(dj.Computed):
    """Execute the volume matching algorithm and store the results.

    Attributes:
        VolumeMatchTask (foreign key): Primary key from `VolumeMatchTask`.
        execution_time (datetime): Execution time of the volume matching task.
        execution_duration (float): Duration of the volume matching task.
    """

    definition = """
    -> VolumeMatchTask
    ---
    execution_time: datetime
    execution_duration: float  # (hr)
    """

    class Transformation(dj.Part):
        """Store transformation matrix data.

        Attributes:
            VolumeMatch (foreign key): Primary key from `VolumeMatch`.
            VolumeMatchTask.Volume (foreign key): Primary key from `VolumeMatchTask.Volume`.
            transformation_matrix (longblob): the transformation matrix to
            transform to the common space.
        """

        definition = """  # transformation matrix
        -> master
        -> VolumeMatchTask.Volume
        ---
        transformation_matrix: longblob  # the transformation matrix to transform to the common space
        """

    class CommonMask(dj.Part):
        """Store common mask ID.

        Attributes:
            common_mask (smallint): Integer value for the common mask ID.
        """

        definition = """
        common_mask: smallint
        """

    class VolumeMask(dj.Part):
        """Store volume mask information.

        Attributes:
            master.CommonMask (foreign key): Primary key from
            `VolumeMatch.CommonMask`.
            VolumeMatchTask.Volume (foreign key): Primary key from
            `VolumeMatchTask.Volume`.
            volume.Segmentation.Mask (foreign key): Primary key from
            `volume.Segmentation.Mask`.
            confidence (float): confidence level of the volume mask.
        """

        definition = """
        -> master.CommonMask
        -> VolumeMatchTask.Volume
        ---
        -> volume.Segmentation.Mask
        confidence: float
        """

    def make(self, key):
        import point_cloud_registration as pcr
        from scipy.stats import gaussian_kde

        vol_keys = (volume.Segmentation & (VolumeMatchTask.Volume & key)).fetch("KEY")

        vol1_points, vol2_points = zip(
            *(volume.Segmentation.Mask & vol_keys).fetch(
                "mask_center_x", "mask_center_y", "mask_center_z"
            )
        )

        vol1_points = np.hstack([*vol1_points])
        vol2_points = np.hstack([*vol2_points])

        tetras1 = pcr.make_normal_tetras(vol1_points)
        tetras2 = pcr.make_normal_tetras(vol2_points)

        pcr.compute_canonical_features(tetras1)
        pcr.remove_common_tetras(tetras1)

        pcr.compute_canonical_features(tetras2)
        pcr.remove_common_tetras(tetras2)

        distances, matches = pcr.match_features(tetras1, tetras2)

        # add complete set of steps once point-cloud-registration algorithm is complete
