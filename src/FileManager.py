import os
from pathlib import Path
from ._globals import TEMP_DIRECTORY
import glob

class FileManager:
    def _build_path(self, mrn, study_uid, modality, series_uid=None, instance_uid=None):
        path = Path(TEMP_DIRECTORY) / mrn / study_uid / modality
        if series_uid:
            path /= series_uid
        if instance_uid:
            path /= instance_uid
        return path



    def query_uid(self, mrn, modality, study_uid, instance_uid, series_uid=None, base_dir=TEMP_DIRECTORY):
        """
        Check if a DICOM file or series exists for the given MRN, study UID, and instance UID.

        Parameters
        ----------
        mrn : str
            Patient MRN (used as root folder name).
        study_uid : str
            StudyInstanceUID.
        instance_uid : str
            SOPInstanceUID of the file to check.
        modality : str
            Modality folder (e.g., "RTDOSE", "RTSTRUCT").
        series_uid : str, optional
            SeriesInstanceUID. If provided, only check that series directory exists and has .dcm files.
        base_dir : Path or str, optional
            Root directory containing MRN folders. Defaults to TEMP_DIRECTORY.

        Returns
        -------
        bool
            True if series or instance file exists, otherwise False.
        """
        base_dir = Path(base_dir)
        search_root = base_dir / str(mrn) / str(study_uid) / modality

        if not search_root.exists():
            return False

        if series_uid:
            series_dir = search_root / str(series_uid)
            if series_dir.is_dir():
                # Check if there are any .dcm files in this series folder
                dcm_files = list(series_dir.glob("*.dcm"))
                return len(dcm_files) > 0
            return False
        else:
            pattern = str(search_root / "**" / f"{instance_uid}.dcm")
            matches = glob.glob(pattern, recursive=True)
            return len(matches) > 0



    def save_dicom(self, dicom):
        path = (
            Path(TEMP_DIRECTORY)
            / dicom.PatientID
            / dicom.StudyInstanceUID
            / dicom.Modality
            / dicom.SeriesInstanceUID
            / f"{dicom.SOPInstanceUID}.dcm"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        dicom.save_as(str(path))
        return path.exists()

