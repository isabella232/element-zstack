import logging
import os
from pathlib import Path
from typing import Optional, Tuple, Union

import numpy as np
from element_interface.utils import find_full_path
from intern import array
from PIL import Image
from PIL.Image import _fromarray_typemap
from requests import HTTPError

from .. import volume

logger = logging.getLogger("datajoint")


class BossDBInterface(array):
    def __init__(
        self,
        channel: Union[Tuple, str],
        session_key: Optional[dict] = None,
        volume_id: Optional[str] = None,
        **kwargs,
    ) -> None:

        try:
            super().__init__(channel=channel, **kwargs)
            self._exists = True
        except HTTPError as e:
            if e.response.status_code == 404 and not kwargs.get("create_new", False):
                self._exists = False
                return
            else:
                raise e

        self._session_key = session_key or dict()

        # If not passed resolution or volume IDs, use the following defaults:
        self._volume_key = dict(
            volume_id=volume_id or self.collection_name + "/" + self.experiment_name,
            resolution_id=self.resolution,
        )

    @property
    def exists(self):
        return self._exists
