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
