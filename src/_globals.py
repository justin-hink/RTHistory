"""Global variables and configuration for DataIngestion"""

import json
import logging
from pathlib import Path
import sys
# -------------------------------------------------------------------------
# Directory setup
# -------------------------------------------------------------------------
if getattr(sys, 'frozen', False):
    # Running inside PyInstaller
    PARENT_DIRECTORY = Path(sys.executable).parent
else:
    # Running as normal script
    PARENT_DIRECTORY = Path(__file__).parent
TEMP_DIRECTORY = PARENT_DIRECTORY / "TEMP"
OUTPUT_DIRECTORY = PARENT_DIRECTORY / "OUTPUT"
LOGS_DIRECTORY = PARENT_DIRECTORY / "logs"

# Ensure necessary directories exist
for directory in [TEMP_DIRECTORY, OUTPUT_DIRECTORY, LOGS_DIRECTORY]:
    directory.mkdir(parents=True, exist_ok=True)

# Log formatter (consistent across scripts)
LOG_FORMATTER = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s:%(lineno)d")

# -------------------------------------------------------------------------
# Default AE variables (populated later if config.json exists)
# -------------------------------------------------------------------------
SELF_AETITLE = ""
SCP_AETITLE = ""
SCP_HOST = ""
SCP_PORT = 0

REMOTE_AET_DICT = {}
SITE_AET_DICT = {}

# -------------------------------------------------------------------------
# Configuration loading
# -------------------------------------------------------------------------
config_path = PARENT_DIRECTORY / "config.json"

if config_path.exists():
    try:
        with open(config_path, "r") as f:
            cfg = json.load(f)

        # Local SCP info
        SCP_AETITLE = cfg["SCP_SERVER"]["AETITLE"]
        SCP_HOST = cfg["SCP_SERVER"]["HOST"]
        SCP_PORT = int(cfg["SCP_SERVER"]["PORT"])

        # Clinical AE info
        CLINICAL_AETITLE = cfg["CLINICAL_SERVER"]["AETITLE"]
        CLINICAL_HOST = cfg["CLINICAL_SERVER"]["HOST"]
        CLINICAL_PORT = int(cfg["CLINICAL_SERVER"]["PORT"])

        # Self AE is same as SCP
        SELF_AETITLE = SCP_AETITLE

        # Build remote AE mappings
        REMOTE_AET_DICT = {
            "CLINICAL": {
                "AETITLE": CLINICAL_AETITLE,
                "HOST": CLINICAL_HOST,
                "PORT": CLINICAL_PORT,
            }
        }

        SITE_AET_DICT = {
            modality: {
                "AETITLE": CLINICAL_AETITLE,
                "HOST": CLINICAL_HOST,
                "PORT": CLINICAL_PORT,
            }
            for modality in [
                "RTRECORD",
                "RTPLAN",
                "RTDOSE",
                "RTSTRUCT",
                "CT",
                "MR",
                "PT",
                "REG",
            ]
        }

    except Exception as e:
        print(f"[Warning] Failed to load config.json: {e}")
else:
    print(f"[Notice] No config.json found at {config_path}. Run configuration first.")

# -------------------------------------------------------------------------
# DICOM Class UID mappings
# -------------------------------------------------------------------------
MODALITY_BY_CLASS_UID = {
    "1.2.840.10008.5.1.4.1.1.2": "CT",
    "1.2.840.10008.5.1.4.1.1.4": "MR",
    "1.2.840.10008.5.1.4.1.1.128": "PT",
    "1.2.840.10008.5.1.4.1.1.481.1": "RTIMAGE",
    "1.2.840.10008.5.1.4.1.1.1": "CR",
    "1.2.840.10008.5.1.4.1.1.481.4": "RTRECORD",
    "1.2.840.10008.5.1.4.1.1.481.5": "RTPLAN",
    "1.2.840.10008.5.1.4.1.1.481.2": "RTDOSE",
    "1.2.840.10008.5.1.4.1.1.481.3": "RTSTRUCT",
    "1.2.840.10008.5.1.4.1.1.66.1": "REG",
}

CLASS_UID_BY_MODALITY = {
    "RTRECORD": "1.2.840.10008.5.1.4.1.1.481.4",
    "RTPLAN": "1.2.840.10008.5.1.4.1.1.481.5",
    "RTDOSE": "1.2.840.10008.5.1.4.1.1.481.2",
    "RTSTRUCT": "1.2.840.10008.5.1.4.1.1.481.3",
    "CT": "1.2.840.10008.5.1.4.1.1.2",
    "MR": "1.2.840.10008.5.1.4.1.1.4",
    "PT": "1.2.840.10008.5.1.4.1.1.128",
    "REG": "1.2.840.10008.5.1.4.1.1.66.1",
}

# -------------------------------------------------------------------------
# Common DICOM object keys
# -------------------------------------------------------------------------
PATIENT_OBJECT_KEYS = [
    "PatientID",
    "PatientName",
    "StudyID",
    "StudyDate",
    "StudyInstanceUID",
    "Modality",
    "SOPClassUID",
    "SeriesInstanceUID",
    "SOPInstanceUID",
    "SeriesDescription",
    "SeriesDate",
    "RTPlanLabel",
    "RTPlanDescription",
    "RTPlanDate",
    "AcquisitionDate",
    "ContentDate",
    "ReferencedSOPClassUID",
    "ReferencedSOPInstanceUID",
    "ApprovalStatus",
    "DoseSummationType",
    "TreatmentDate",
    "TreatmentTime",
    "TreatmentDeliveryType",
    "TreatmentTerminationStatus",
    "CurrentFractionNumber",
    "ReferencedRTStructureSetSequence",
]
