import datajoint as dj
import numpy as np
import importlib
import inspect
import pathlib
import hashlib
import uuid
from collections.abc import Callable

from element_interface.utils import dict_to_uuid, find_full_path, find_root_directory

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
    schema.activate(
        schema_name,
        create_schema=create_schema,
        create_tables=create_tables,
        add_objects=volume.__dict__,
    )


@schema
class VolumeMatchingTask(dj.Manual):
    definition = """  # 
    task_id: uuid
    ---
    task_name: varchar(256)
    """

    class VolumeA(dj.Part):
        definition = """
        -> master
        ---
        -> volume.Segmentation
        """

    class VolumeB(dj.Part):
        definition = """
        -> master
        ---
        -> volume.Segmentation
        """

    @classmethod
    def insert1(cls, volAB_keys, **kwargs):
        """
        Args:
            volAB_keys (tuple): a tuple of (volA_key, volB_key)
        """
        volA_key, volB_key = volAB_keys
        volA_key = (volume.Segmentation & volA_key).fetch1("KEY")
        volB_key = (volume.Segmentation & volB_key).fetch1("KEY")
        assert volA_key != volB_key, "The two specified volumes are identical"

        hashed = hashlib.md5()
        for k, v in sorted(volA_key.items() + volB_key.items()):
            hashed.update(str(k).encode())
            hashed.update(str(v).encode())
        task_id = uuid.UUID(hex=hashed.hexdigest())

        mkey = {'task_id': task_id}
        if cls & mkey:
            assert (cls.VolumeA & mkey & volA_key)
            assert (cls.VolumeB & mkey & volB_key)
            return

        with cls.connection.transaction:
            super().insert1(cls(), mkey, **kwargs)
            cls.VolumeA.insert1({**mkey, **volA_key})
            cls.VolumeB.insert1({**mkey, **volB_key})


@schema
class VolumeMatching(dj.Computed):
    definition = """
    -> VolumeMatchingTask
    ---
    execution_time: datetime
    execution_duration: float  # (hr)
    transformation_matrix: longblob  # the transformation matrix to transform VolA into VolB space
    """

    class CommonMask(dj.Part):
        definition = """
        common_mask: smallint
        """

    class VolumeAMask(dj.Part):
        definition = """
        -> master.CommonMask
        -> VolumeMatchingTask.VolumeA
        ---
        -> volume.Segmentation.Mask
        """

    class VolumeBMask(dj.Part):
        definition = """
        -> master.CommonMask
        -> VolumeMatchingTask.VolumeB
        ---
        -> volume.Segmentation.Mask
        """

    def make(self, key):
        import point_cloud_registration as pcr
        from scipy.stats import gaussian_kde

        volA_key = (volume.Segmentation & (VolumeMatchingTask.VolumeA & key)).fetch1('KEY')
        volB_key = (volume.Segmentation & (VolumeMatchingTask.VolumeA & key)).fetch1('KEY')

        volA_points = (volume.Segmentation.Mask & volA_key).fetch(
            'mask_center_x', 'mask_center_y', 'mask_center_z')
        volB_points = (volume.Segmentation.Mask & volB_key).fetch(
            'mask_center_x', 'mask_center_y', 'mask_center_z')

        volA_points = np.hstack([*volA_points])
        volB_points = np.hstack([*volB_points])

        tetras1 = pcr.make_normal_tetras(volA_points)
        tetras2 = pcr.make_normal_tetras(volB_points)

        pcr.compute_canonical_features(tetras1)
        pcr.remove_common_tetras(tetras1)

        pcr.compute_canonical_features(tetras2)
        pcr.remove_common_tetras(tetras2)

        distances, matches = pcr.match_features(tetras1, tetras2)

        # add complete set of steps once point-cloud-registration algorithm is complete
