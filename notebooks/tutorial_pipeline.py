import os
import pathlib
import datajoint as dj
from collections.abc import Sequence
from typing import List
from element_interface.utils import find_full_path
from element_lab import lab
from element_lab.lab import Lab, Location, Project, Protocol, Source, User
from element_animal import subject, surgery
from element_animal.subject import Subject
from element_session import session_with_id as session
from element_calcium_imaging import imaging, scan
from element_zstack import volume, bossdb


__all__ = [
    "db_prefix",
    "lab",
    "scan",
    "imaging",
    "session",
    "subject",
    "surgery",
    "volume",
    "bossdb",
    "get_volume_root_data_dir",
    "get_volume_tif_file",
]

if "custom" not in dj.config:
    dj.config["custom"] = {}

# overwrite dj.config['custom'] values with environment variables if available

dj.config["custom"]["database.prefix"] = os.getenv(
    "DATABASE_PREFIX", dj.config["custom"].get("database.prefix", "")
)

dj.config["custom"]["volume_root_data_dir"] = os.getenv(
    "VOLUME_ROOT_DATA_DIR", dj.config["custom"].get("volume_root_data_dir", "")
)

db_prefix = dj.config["custom"].get("database.prefix", "")


def get_volume_root_data_dir() -> List[str]:
    """Return root directory for volumetric data in dj.config

    Returns:
        path (any): List of path(s) if available or None
    """
    vol_root_dirs = dj.config.get("custom", {}).get("volume_root_data_dir", None)
    if not vol_root_dirs:
        return None
    elif not isinstance(vol_root_dirs, Sequence):
        return list(vol_root_dirs)
    else:
        return pathlib.Path(vol_root_dirs)


def get_volume_tif_file(scan_key):
    """Retrieve the ScanImage file associated with a given Scan.

    Args:
        scan_key (dict): Primary key from Scan.

    Returns:
        path (str): Absolute path of the scan file.

    Raises:
        FileNotFoundError: If the tiff file(s) are not found.
    """
    # Folder structure: root / subject / session / .tif (raw)
    sess_dir = find_full_path(
        get_volume_root_data_dir(),
        pathlib.Path((session.SessionDirectory & scan_key).fetch1("session_dir")),
    )

    tiff_filepaths = [fp.as_posix() for fp in sess_dir.rglob("*.tif")]

    if tiff_filepaths:
        assert (
            len(tiff_filepaths) == 1
        ), "More than 1 `.tif` file in file path. Please ensure the session directory contains only 1 image file."
        return tiff_filepaths[0]
    else:
        raise FileNotFoundError(f"No tiff file found in {sess_dir}")


# ---------------------------------- Activate schemas ----------------------------------

lab.activate(db_prefix + "lab")
subject.activate(db_prefix + "subject", linking_module=__name__)
surgery.activate(db_prefix + "surgery", linking_module=__name__)

Experimenter = lab.User
session.activate(db_prefix + "session", linking_module=__name__)

Equipment = Device
Session = session.Session
SessionDirectory = session.SessionDirectory
imaging.activate(db_prefix + "imaging", db_prefix + "scan", linking_module=__name__)

Mask = imaging.Segmentation.Mask
Scan = scan.Scan
volume.activate(db_prefix + "volume", linking_module=__name__)
bossdb.activate(db_prefix + "bossdb", linking_module=__name__)
