"""
Class module for DICOM SCU
"""

import time
import logging
from typing import List
from pydicom.sequence import Sequence
from datetime import datetime  # , timedelta
from pydicom.dataset import Dataset
from rosamllib.networking import QueryRetrieveSCU
from ._globals import (
    LOGS_DIRECTORY,
    LOG_FORMATTER,
    PATIENT_OBJECT_KEYS,
    CLASS_UID_BY_MODALITY,
    MODALITY_BY_CLASS_UID,
    SCP_AETITLE,
    REMOTE_AET_DICT,
)

class MySCU(QueryRetrieveSCU):
    # SCU QUERY TO FIND ALL RTRECORDS FOR A DAY
    # THIS LETS US SEND RTPLANS TO QUEUE
    # """QUERY for RTRECORDS by DAY"""
    def find_treatment_records(self, mrn=None) -> List:  # day = "YYYY-MM-DD"
        """Queries all RTRECORDS for a given day.

        Parameters
        ----------
        day : str
            The day to query (formatted as "YYYY-MM-DD")

        mrn : str
            The mrn to query

        Returns
        -------
        List
            A list of dictionaries for RTRECORDS with relevant DICOM tags as keys
            and their corresponding values.
        """

        # date.fromisoformat('YYYY-MM-DD')
        self.logger.info(
            f"QUERYING TREATMENTS FOR PATIENT_ID {mrn}"
        )
        result = {}
        # go through plans and create dicom tree
        study_ds = Dataset()
        study_ds.QueryRetrieveLevel = "IMAGE"
        study_ds.PatientID = mrn  # MRN '1111111' '#######'
        study_ds.PatientName = ""  # "DOE^JOHN"
        study_ds.StudyID = ""
        study_ds.StudyInstanceUID = ""
        study_ds.SeriesInstanceUID = ""
        study_ds.Modality = "RTRECORD"
        study_ds.SOPInstanceUID = ""
        study_ds.SOPClassUID = ["1.2.840.10008.5.1.4.1.1.481.4"]
        study_ds.TreatmentDate = ""
        study_ds.TreatmentTime = ""
        study_ds.TreatmentTerminationStatus = ""
        study_ds.ReferencedSOPClassUID = ""
        study_ds.ReferencedSOPInstanceUID = ""
        result["patients"] = []
        # Perform a Study Root Query/Retrieve operation with specified query dataset
        responses = self.c_find(ae_name="VMSDBD2", query=study_ds)
        counter = 1
        for response in responses:
            if response is not None:
                ds = response
                add_record = True
                # Use with RT Beams Treatment Record
                # Only grabs one RTRecord per RTPlan
                list_of_patients = [
                    a_dict["ReferencedSOPInstanceUID"]
                    for a_dict in result["patients"]
                ]
                if list_of_patients is not None:
                    if ds.ReferencedSOPInstanceUID in list_of_patients:
                        add_record = False

                if add_record:
                    pt_obj = {}
                    pt_obj["index"] = counter
                    counter += 1

                    for key in PATIENT_OBJECT_KEYS:
                        if key in ds.dir():
                            if key == "PatientName":
                                pt_obj[key] = getattr(
                                    ds, key
                                ).family_comma_given()
                            else:
                                pt_obj[key] = getattr(ds, key)

                    result["patients"].append(pt_obj)
            else:
                self.logger.info(f"No RTRecord query responses for patient {mrn}.")
        return sorted(result["patients"], key=lambda x: x["TreatmentTime"])

        # C-FIND THE RTDOSE USING THE PLAN AS REFERENCEDSOPINSTANCEUID

    def query_dicom_rt(
        self,
        mrn: str,
        study_uid: str,
        inst_uid: str,
        class_uid: str,
        series_uid: str = None,
    ) -> List:
        """Queries for a DICOM object with the given parameters.

        Parameters
        ----------
        mrn : str
            The patient ID
        study_uid : str
            The Study Instance UID
        inst_uid : str
            The SOP Instance UID
        class_uid : str
            The SOP Class UID
        level : str
            The Query/Retrieve level ("SERIES" or "IMAGE")
        seriesInst_uid : str, optional
            The Series Instance UID, by default None
        item_aet_dict : dict, optional
            The AET information for this item

        Returns
        -------
        List
            A list of dictionaries for the DICOM object being queried with
            relevant DICOM tags as keys and their corresponding values.
        """
        def build_query_study_ds(class_uid, mrn, study_uid, inst_uid=None, series_uid=None):
            study_ds = Dataset()
            study_ds.QueryRetrieveLevel = "IMAGE"  # or "SERIES", depending on your use
            study_ds.PatientID = mrn
            study_ds.StudyInstanceUID = study_uid

            modality = MODALITY_BY_CLASS_UID[class_uid]
            study_ds.Modality = modality
            study_ds.SOPClassUID = class_uid
            study_ds.SeriesInstanceUID = ""  # default

            # SOPInstanceUID logic
            if modality in ["RTPLAN", "CT"]:
                study_ds.SOPInstanceUID = inst_uid or ""

            # Reference sequences
            if modality in ["RTDOSE", "RTRECORD"]:
                study_ds.SOPInstanceUID = ""
                ref_seq = Sequence()
                ref_ds = Dataset()
                ref_ds.ReferencedSOPClassUID = CLASS_UID_BY_MODALITY["RTPLAN"]
                ref_ds.ReferencedSOPInstanceUID = inst_uid
                ref_seq.append(ref_ds)
                study_ds.ReferencedRTPlanSequence = ref_seq

            elif modality == "RTSTRUCT":
                ref_seq = Sequence()
                ref_ds = Dataset()
                ref_ds.ReferencedSOPClassUID = CLASS_UID_BY_MODALITY["CT"]
                ref_ds.ReferencedSOPInstanceUID = inst_uid
                ref_seq.append(ref_ds)
                study_ds.ReferencedRTPlanSequence = ref_seq

            elif modality == "RTPLAN":
                study_ds.SeriesInstanceUID = series_uid or ""
                ref_seq = Sequence()
                ref_ds = Dataset()
                ref_ds.ReferencedSOPClassUID = CLASS_UID_BY_MODALITY["RTSTRUCT"]
                ref_ds.ReferencedSOPInstanceUID = ""
                ref_seq.append(ref_ds)
                study_ds.ReferencedRTStructureSetSequence = ref_seq

            return study_ds

        self.logger.info(f"QUERYING {MODALITY_BY_CLASS_UID[class_uid]}")
        study_ds = build_query_study_ds(class_uid, mrn, study_uid, inst_uid, series_uid)
        result = {}

        result["patients"] = []
        # Perform a Study Root Query/Retrieve operation with specified query dataset
        responses = self.c_find(ae_name="VMSDBD2", query=study_ds)
        counter = 1
        for response in responses:
            if response is not None:
                ds = response
                pt_obj = {}
                pt_obj["index"] = counter
                counter += 1

                for key in PATIENT_OBJECT_KEYS:
                    if key in ds.dir():

                        if key == "PatientName":
                            pt_obj[key] = getattr(
                                ds, key
                            ).family_comma_given()

                        elif key == "SOPClassUID":
                            if (
                                getattr(ds, key)
                                == "1.2.840.10008.5.1.4.1.1.481.2"
                            ):
                                pt_obj[key] = "RT Dose"

                            elif (
                                getattr(ds, key)
                                == "1.2.840.10008.5.1.4.1.1.481.4"
                            ):
                                pt_obj[key] = (
                                    "RT Beams Treatment Record"
                                )

                            else:
                                pt_obj[key] = getattr(ds, key)

                        elif key == "ReferencedRTStructureSetSequence":
                            pt_obj[
                                "ReferencedRTStructSOPInstanceUID"
                            ] = getattr(ds, key)[
                                0
                            ].ReferencedSOPInstanceUID
                            pt_obj["ReferencedRTStructSOPClassUID"] = (
                                getattr(ds, key)[
                                    0
                                ].ReferencedRTStructSOPClassUID
                            )

                        else:
                            pt_obj[key] = getattr(ds, key)

                    result["patients"].append(pt_obj)
            else:
                self.logger.info(f"No C-FIND Responses for {MODALITY_BY_CLASS_UID[class_uid]}.")

        return result["patients"]

    # C-MOVE DICOM TO SCP
    def move_dicom_to_scp(
            self,
            mrn: str,
            study_uid: str,
            class_uid: str,
            instance_uid: str,
            level: str = "IMAGE",
        ) -> List | None:
            """Move DICOM to SCP.

            Parameters
            ----------
            mrn : str
                The Patient ID
            study_uid : str
                The Study Instance UID
            class_uid : str
                The SOP Class UID
            instance_uid : str
                The SOP Instance UID
            level : str
                Query/Retrieve Level
            item_aet_dict : dict
                Dictionary containing item AET information

            Returns
            -------
            List | None
                All the status messages received while executing the move.
                Return None if C-Move request is unsuccesful.
            """

            temp_ds = Dataset()
            temp_ds.PatientID = str(mrn)
            temp_ds.QueryRetrieveLevel = level
            temp_ds.StudyInstanceUID = str(study_uid)
            # temp_ds.SOPInstanceUID = str(instance_uid)
            temp_ds.SOPClassUID = str(class_uid)
            if level == "SERIES":
                temp_ds.SeriesInstanceUID = str(instance_uid)
            else:
                temp_ds.SOPInstanceUID = str(instance_uid)
            self.c_move(ae_name=REMOTE_AET_DICT["CLINICAL"]["AETITLE"], query=temp_ds, destination_ae=SCP_AETITLE)