import datajoint as dj
import numpy as np
import hashlib
import uuid

from element_interface.utils import dict_to_uuid

from . import volume


schema = dj.Schema()

_linking_module = None


def activate(
    schema_name: str,
    *,
    create_schema: bool = True,
    create_tables: bool = True
):
    """Activate this schema.
    Args:
        schema_name (str): Schema name on the database server to activate the
            `imaging_report` schema
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
    definition = """  # 
    volume_match_task: uuid
    """

    class Volume(dj.Part):
        definition = """
        -> master
        -> volume.Segmentation
        """

    @classmethod
    def insert1(cls, vol_keys, **kwargs):
        """
        Args:
            vol_keys (tuple): a tuple of (volA_key, volB_key)
        """
        assert len(vol_keys) == 2, f"Volume match task only support matching two volumes, {len(vol_keys)} are provided"
        vol_keys = [(volume.Segmentation & k).fetch1("KEY") for k in vol_keys]
        assert len(set(vol_keys)) == 2, "The two specified volumes are identical"

        hashed = hashlib.md5()
        [hashed.update(str(k).encode()) for k in sorted([dict_to_uuid(k) for k in vol_keys])]

        mkey = {'volume_match_task': uuid.UUID(hex=hashed.hexdigest())}
        if cls & mkey:
            assert len(cls.Volume & mkey & vol_keys) == 2
            return

        with cls.connection.transaction:
            super().insert1(cls(), mkey, **kwargs)
            cls.Volume.insert({**mkey, **k} for k in vol_keys)


@schema
class VolumeMatch(dj.Computed):
    definition = """
    -> VolumeMatchTask
    ---
    execution_time: datetime
    execution_duration: float  # (hr)
    """

    class Transformation(dj.Part):
        definition = """  # transformation matrix
        -> master
        -> VolumeMatchingTask.Volume
        ---
        transformation_matrix: longblob  # the transformation matrix to transform to the common space
        """

    class CommonMask(dj.Part):
        definition = """
        common_mask: smallint
        """

    class VolumeMask(dj.Part):
        definition = """
        -> master.CommonMask
        -> VolumeMatchingTask.Volume
        ---
        -> volume.Segmentation.Mask
        confidence: float
        """

    def make(self, key):
        import point_cloud_registration as pcr
        from scipy.stats import gaussian_kde

        vol_keys = (volume.Segmentation & (VolumeMatchTask.Volume & key)).fetch('KEY')

        vol1_points, vol2_points = zip(*(volume.Segmentation.Mask & vol_keys).fetch(
            'mask_center_x', 'mask_center_y', 'mask_center_z'))

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
