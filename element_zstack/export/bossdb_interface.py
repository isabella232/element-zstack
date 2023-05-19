import logging
from typing import Optional, Tuple
import numpy as np
from intern import array
from intern.convenience.array import _parse_bossdb_uri
from intern.remote.boss import BossRemote
from intern.resource.boss.resource import (
    ChannelResource,
    CollectionResource,
    CoordinateFrameResource,
    ExperimentResource,
)
from requests import HTTPError
from tqdm.auto import tqdm

logger = logging.getLogger("datajoint")


class BossDBUpload:
    """Upload data to bossdb from a DataJoint pipeline."""

    def __init__(
        self,
        url: str,
        volume_data: np.ndarray,  # Numpy array of the volumetric data
        data_description: str,
        voxel_size: Tuple[int, int, int],  # voxel size in ZYX order
        voxel_units: str,  # The size units of a voxel
        upload_increment: Optional[int] = 16,  # How many z slices to upload at once
        retry_max: Optional[int] = 3,  # Number of retries to upload a single increment
        overwrite: Optional[bool] = False,  # Overwrite existing data
    ):
        """Required information for data upload to bossdb.

        Args:
            url (str): Bossdb URL where data will be uploaded.
            volume_data (np.ndarray): Numpy array of the volumetric data
            data_description (str): either `image` or `annotation`.
            voxel_size (Tuple[int, int, int]): Voxel size of the image in z,y,x
            format.
            voxel_units (str): Voxel units as string.
            upload_increment (int, optional): Number of z slices to upload at
            once.
            retry_max (int, optional): Number of retries to upload a single
            increment.
            overwrite (bool, optional): Overwrite existing data.
        """

        self._url = url
        self.url_bits = _parse_bossdb_uri(url)
        self._volume_data = volume_data
        self._voxel_size = tuple(float(i) for i in voxel_size)
        self._voxel_units = voxel_units
        self._data_description = data_description
        self._upload_increment = upload_increment
        self._retry_max = retry_max
        self._overwrite = overwrite
        self.description = "Uploaded via DataJoint"
        self._resources = dict()
        self._shape_zyx = tuple(int(i) for i in self._volume_data.shape)

        try:
            type(array(self._url))
        except HTTPError:
            self.url_exists = False
        else:
            self.url_exists = True
        if not overwrite and self.url_exists:
            logger.warning(
                f"Dataset already exists at {self._url}\n"
                + " To overwrite, set `overwrite` to True"
            )
            return

        if not self.url_exists:
            self.try_create_new()

    def upload(self):
        """Upload data to bossdb."""
        z_max = self._shape_zyx[0]
        boss_dataset = array(
            self._url,
            extents=self._shape_zyx,
            dtype=str(self._volume_data.dtype),
            voxel_size=self._voxel_size,
            voxel_unit=self._voxel_units,
        )
        for i in tqdm(range(0, z_max, self._upload_increment)):
            if i + self._upload_increment > self._shape_zyx[0]:
                # We're at the end of the stack, so upload the remaining images.
                images = self._volume_data[i : self._shape_zyx[0], :, :]
                retry_count = 0
                while True:
                    try:
                        boss_dataset[
                            i : i + images.shape[0],
                            0 : images.shape[1],
                            0 : images.shape[2],
                        ] = images
                        break
                    except Exception as e:
                        print(f"Error uploading chunk {i}-{i + images.shape[0]}: {e}")
                        retry_count += 1
                        if retry_count > self._retry_max:
                            raise e
                        print("Retrying...")
                        continue
            else:
                images = self._volume_data[i : i + self._upload_increment]
                retry_count = 0
                while True:
                    try:
                        boss_dataset[
                            i : i + images.shape[0],
                            0 : images.shape[1],
                            0 : images.shape[2],
                        ] = images
                        break
                    except Exception as e:
                        print(f"Error uploading chunk {i}-{i + images.shape[0]}: {e}")
                        retry_count += 1
                        if retry_count > self._retry_max:
                            raise e
                        print("Retrying...")
                        continue

    @property
    def resources(self):
        """Create the resources objects required for uploading data to bossdb."""
        # Default resources for creating channels
        coord_name = f"CF_{self.url_bits.collection}_{self.url_bits.experiment}"
        if not self._resources:
            self._resources = dict(
                collection=CollectionResource(
                    name=self.url_bits.collection, description=self.description
                ),
                coord_frame=CoordinateFrameResource(
                    name=coord_name,
                    description=self.description,
                    x_start=0,
                    x_stop=self._shape_zyx[2],
                    y_start=0,
                    y_stop=self._shape_zyx[1],
                    z_start=0,
                    z_stop=self._shape_zyx[0],
                    x_voxel_size=self._voxel_size[2],
                    y_voxel_size=self._voxel_size[1],
                    z_voxel_size=self._voxel_size[0],
                ),
                experiment=ExperimentResource(
                    name=self.url_bits.experiment,
                    collection_name=self.url_bits.collection,
                    coord_frame=coord_name,
                    description=self.description,
                ),
                channel_resource=ChannelResource(
                    name=self.url_bits.channel,
                    collection_name=self.url_bits.collection,
                    experiment_name=self.url_bits.experiment,
                    type=self._data_description,
                    description=self.description,
                    datatype=str(self._volume_data.dtype),
                ),
                channel=ChannelResource(
                    name=self.url_bits.channel,
                    collection_name=self.url_bits.collection,
                    experiment_name=self.url_bits.experiment,
                    type=self._data_description,
                    description=self.description,
                    datatype=str(self._volume_data.dtype),
                    sources=[],
                ),
            )
        return self._resources

    def try_create_new(self):
        """Create new resources on bossdb."""
        remote = BossRemote()

        # Make collection
        _ = self._get_or_create(remote=remote, obj=self.resources["collection"])

        # Make coord frame
        true_coord_frame = self._get_or_create(
            remote=remote, obj=self.resources["coord_frame"]
        )

        # Set Experiment based on coord frame
        experiment = self.resources["experiment"]
        experiment.coord_frame = true_coord_frame.name
        _ = self._get_or_create(remote=remote, obj=experiment)

        # Set channel based on resource
        channel_resource = self._get_or_create(
            remote=remote, obj=self.resources["channel_resource"]
        )
        channel = self.resources["channel"]
        channel.sources = [channel_resource.name]
        _ = self._get_or_create(remote=remote, obj=channel)

    def _get_or_create(self, remote, obj):
        """Check if a resource exists on bossdb."""
        try:
            result = remote.get_project(obj)
        except HTTPError:
            logger.info(f"Creating {obj.name}")
            result = remote.create_project(obj)
        return result
