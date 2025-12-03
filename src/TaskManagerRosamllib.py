# task_manager_sqlalchemy.py
import sys
import os
import json
from pathlib import Path
from queue import Queue
from collections import namedtuple
from datetime import datetime

import pydicom
from .FileManager import FileManager
from .QueryRetrieveSCU_rosamllib import MySCU
from .StoreSCPRosamllib import MyStoreSCP
from ._globals import (
    TEMP_DIRECTORY,
    MODALITY_BY_CLASS_UID,
)

from .logger_setup import TaskManager_task_logger  # SQLAlchemy logger

class TaskManager:
    # Assign the SQLAlchemy logger at class level
    task_logger = TaskManager_task_logger

    def __init__(
        self,
        scu: MySCU,
        scp: MyStoreSCP,
        continue_: str = None,
        mrn: str = None,
        log_level_cli: str = None,
    ) -> None:
        self.scu = scu
        self.scp = scp
        self.continue_ = continue_
        self.mrn = mrn
        self.File_Manager = FileManager()
        self.log_level_cli = log_level_cli
        self.task_queue = Queue()
        self.Item = namedtuple(
            "Item",
            [
                "PatientID",
                "StudyInstanceUID",
                "SOPClassUID",
                "SeriesInstanceUID",
                "Modality",
                "SOPInstanceUID",
                "Attempt_No",
            ],
        )
        self.list_file = Path(TEMP_DIRECTORY / "list_of_file_paths.txt")
        
        # Optional: if you want per-MRN database separation, you could wrap here
        # self.task_logger.set_mrn(self.mrn)


    def run(self) -> None:
        """
        Run the TaskManager based on the provided arguments.

        This method runs the TaskManager based on the provided arguments. If
        `self.continue_` is set, it calls `self.run_continuation()`. If `self.mrn`
        is set, it calls `self.run_from_mrn()`. If neither is set, it exits with
        an error message.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        if self.mrn:
            # print("Up To Run Works")
            self.run_from_mrn()
        else:
            sys.exit("The arguments passed are not valid.")

    def run_from_mrn(self):
        """_summary_"""
        self.scp.start()
        results = self.scu.find_treatment_records(mrn=self.mrn)
        for result in results:
            self.task_queue.put(
                self.Item(
                    result["PatientID"],
                    result["StudyInstanceUID"],
                    result["ReferencedSOPClassUID"],
                    "",
                    "RTPLAN",
                    result["ReferencedSOPInstanceUID"],
                    0,
                )
            )
        while not self.task_queue.empty():
            item = self.task_queue.get()
            self.run_task(item)
        # print("Puts all the results to task_queue")

    def run_task(self, item):
        """_summary_

        Parameters
        ----------
        item : _type_
            _description_
        """
        if item.Attempt_No < 10:
            if item.Modality == "RTPLAN":
                self.run_plan(item)
            elif item.Modality == "RTDOSE":
                self.run_dose(item)
            elif item.Modality == "RTRECORD":
                self.run_record(item)
            elif item.Modality == "RTSTRUCT":
                self.run_struct(item)
            elif item.Modality in ["CT", "MR", "PT"]:
                self.run_image(item)
            else:
                TaskManager.task_logger.error(
                    f"No functionality for {item.Modality} yet."
                )
        else:
            TaskManager.task_logger.error(
                f"Too many attempts for {item.Modality} -- "
                + f"PatientID={item.PatientID}, "
                + f"StudyInstanceUID={item.StudyInstanceUID}, "
                + f"SeriesInstanceUID{item.SeriesInstanceUID}, "
                + f"SOPInstanceUID={item.SOPInstanceUID}, "
                + f"Attempt_No={item.Attempt_No}"
            )

    def run_plan(self, item):
        """_summary_

        Parameters
        ----------
        item : _type_
            _description_
        """
        TaskManager.task_logger.info(
            f"Attempting task for {item.Modality} -- "
            + f"PatientID={item.PatientID}, "
            + f"StudyInstanceUID={item.StudyInstanceUID}, "
            + f"SeriesInstanceUID{item.SeriesInstanceUID}, "
            + f"SOPInstanceUID={item.SOPInstanceUID}, "
            + f"Attempt_No={item.Attempt_No}"
        )
        # Check if it is in temp folder
        status_temp = self.File_Manager.query_uid(
            item.PatientID,
            item.Modality,
            item.StudyInstanceUID,
            item.SOPInstanceUID,
        )
        if not status_temp:
            # TaskManager.task_logger.info(f"")
            # Move RTPLAN to SCP
            status = self.scu.move_dicom_to_scp(
                item.PatientID,
                item.StudyInstanceUID,
                item.SOPClassUID,
                item.SOPInstanceUID,
                "IMAGE",
            )
            try:
                if not status.status:
                    TaskManager.task_logger.info(
                        f"Successfully moved {item.Modality} with "
                        + f"PatientID={item.PatientID}, "
                        + f"SOPInstanceUID={item.SOPInstanceUID} to SCP."
                    )
                    # C-Move successful, get Referenced RTSTRUCT info
                    try:
                        ds = self.scp.received_dicom[0]
                    except IndexError as e:
                        TaskManager.task_logger.error(
                            f"Did not receive {item.Modality} for "
                            + f"PatientID={item.PatientID} with "
                            + f"SOPInstanceUID={item.SOPInstanceUID} "
                            + f"{e}"
                        )
                        self.task_queue.put(
                            self.Item(
                                item.PatientID,
                                item.StudyInstanceUID,
                                item.SOPClassUID,
                                "",
                                item.Modality,
                                item.SOPInstanceUID,
                                item.Attempt_No + 1,
                            )
                        )
                        self.scp.received_dicom = []
                        return
                    self.scp.received_dicom = []
                    # Enque Referenced RTSTRUCT to Queue
                    try:
                        rt_struct_item = self.Item(
                            ds.PatientID,
                            ds.StudyInstanceUID,
                            ds.ReferencedStructureSetSequence[0].ReferencedSOPClassUID,
                            "",
                            "RTSTRUCT",
                            ds.ReferencedStructureSetSequence[
                                0
                            ].ReferencedSOPInstanceUID,
                            0,
                        )
                        self.task_queue.put(rt_struct_item)
                    except AttributeError as e:
                        TaskManager.task_logger.error(
                            "Couldn't get necessary DICOM tags from RTPLAN "
                            + f"for PatientID={ds.PatientID} with "
                            + f"SOPInstanceUID={ds.SOPInstanceUID}"
                            + f"{e}"
                        )
                    # Query for RTDOSE that reference the RTPLAN
                    results = self.scu.query_dicom_rt(
                        item.PatientID,
                        item.StudyInstanceUID,
                        item.SOPInstanceUID,
                        "1.2.840.10008.5.1.4.1.1.481.2",
                        "",
                    )
                    # For all C-FIND RTDOSE result, enqueue to Queue
                    for result in results:
                        TaskManager.task_logger.info(
                            "Successfully found RTDOSE with "
                            + f"SOPInstanceUID={result['SOPInstanceUID']}"
                        )
                        self.task_queue.put(
                            self.Item(
                                result["PatientID"],
                                result["StudyInstanceUID"],
                                "1.2.840.10008.5.1.4.1.1.481.2",
                                result["SeriesInstanceUID"],
                                "RTDOSE",
                                result["SOPInstanceUID"],
                                0,
                            )
                        )
                    # Query for RTRECORDS that reference the RTPLAN
                    results = self.scu.query_dicom_rt(
                        item.PatientID,
                        item.StudyInstanceUID,
                        item.SOPInstanceUID,
                        "1.2.840.10008.5.1.4.1.1.481.4",
                        "",
                    )

                    if results:
                        for result in results:
                            TaskManager.task_logger.info(
                                "Successfully found RTRECORDS with "
                                + f"SeriesInstanceUID={result['SeriesInstanceUID']}"
                            )
                            self.task_queue.put(
                                self.Item(
                                    result["PatientID"],
                                    result["StudyInstanceUID"],
                                    "1.2.840.10008.5.1.4.1.1.481.4",
                                    result["SeriesInstanceUID"],
                                    "RTRECORD",
                                    "",
                                    0,
                                )
                            )
                    else:
                        TaskManager.task_logger.debug(
                            "Did not find RTRECORDS that reference the RTPLAN with "
                            + f"PatientID={item.PatientID} and "
                            + f"SOPInstanceUID={item.SOPInstanceUID}"
                        )

                else:
                    TaskManager.task_logger.error(
                        f"Failed to move {item.Modality} with "
                        + f"SOPInstanceUID={item.SOPInstanceUID} to SCP."
                    )
                    self.task_queue.put(
                        self.Item(
                            item.PatientID,
                            item.StudyInstanceUID,
                            item.SOPClassUID,
                            "",
                            "RTPLAN",
                            item.SOPInstanceUID,
                            item.Attempt_No + 1,
                        )
                    )
            except TypeError as e:
                TaskManager.task_logger.info(
                    f"Error putting {item.Modality} to queue: " + f"{e}" 
                )
        else:
            TaskManager.task_logger.info(
                f"SOPInstanceUID={item.SOPInstanceUID} already in temp_file"
            )

    def run_struct(self, item):
        """_summary_

        Parameters
        ----------
        item : _type_
            _description_
        """
        TaskManager.task_logger.info(
            f"Attempting task for {item.Modality} -- "
            + f"PatientID={item.PatientID}, "
            + f"StudyInstanceUID={item.StudyInstanceUID}, "
            + f"SeriesInstanceUID={item.SeriesInstanceUID}, "
            + f"SOPInstanceUID={item.SOPInstanceUID}, "
            + f"Attempt_No={item.Attempt_No}"
        )
        # Check if it is in temp folder
        status_temp = self.File_Manager.query_uid(
            item.PatientID,
            item.Modality,
            item.StudyInstanceUID,
            item.SOPInstanceUID,
        )
        if not status_temp:
            status = self.scu.move_dicom_to_scp(
                item.PatientID,
                item.StudyInstanceUID,
                item.SOPClassUID,
                item.SOPInstanceUID,
                "IMAGE",
            )
            try:
                if not status.status:                    
                    TaskManager.task_logger.info(
                        f"Successfully moved {item.Modality} with "
                        + f"SOPInstanceUID={item.SOPInstanceUID} to SCP."
                    )
                    try:
                        ds = self.scp.received_dicom[0]
                    except IndexError as e:
                        TaskManager.task_logger.error(
                            f"Did not receive {item.Modality} for "
                            + f"PatientID={item.PatientID} with "
                            + f"SOPInstanceUID={item.SOPInstanceUID} "
                            + f"{e}"
                        )
                        self.task_queue.put(
                            self.Item(
                                item.PatientID,
                                item.StudyInstanceUID,
                                item.SOPClassUID,
                                "",
                                item.Modality,
                                item.SOPInstanceUID,
                                item.Attempt_No + 1,
                            )
                        )
                        self.scp.received_dicom = []
                        return
                    self.scp.received_dicom = []
                    ROIContourSequence_Index = 0
                    try:
                        while (
                            "ContourSequence"
                            not in ds.ROIContourSequence[ROIContourSequence_Index].dir()
                        ):
                            ROIContourSequence_Index += 1
                        ct_slice_item = self.Item(
                            ds.PatientID,
                            ds.StudyInstanceUID,
                            ds.ROIContourSequence[ROIContourSequence_Index]
                            .ContourSequence[0]
                            .ContourImageSequence[0]
                            .ReferencedSOPClassUID,
                            "",
                            MODALITY_BY_CLASS_UID[
                                ds.ROIContourSequence[ROIContourSequence_Index]
                                .ContourSequence[0]
                                .ContourImageSequence[0]
                                .ReferencedSOPClassUID
                            ],
                            ds.ROIContourSequence[ROIContourSequence_Index]
                            .ContourSequence[0]
                            .ContourImageSequence[0]
                            .ReferencedSOPInstanceUID,
                            0,
                        )
                        self.task_queue.put(ct_slice_item)
                    except Exception as e:
                        try:
                            TaskManager.task_logger.error(
                                "Could not find any data "
                                + f"in ROIContourSequence for {item.PatientID} "
                                + f"and SOPInstanceUID={ds.SOPInstanceUID}"
                                + (str(e))
                            )
                        except Exception as ee:
                            TaskManager.task_logger.error(str(ee))
                        # LOG THIS ERROR?

                    # status = self.scu.move_dicom_to_scp(
                    #     item.PatientID,
                    #     item.StudyInstanceUID,
                    #     item.SOPClassUID,
                    #     item.SOPInstanceUID,
                    #     "SERIES",
                    # )
                    # try:
                    #     if status:
                    #         TaskManager.task_logger.error(
                    #             f"Failed to move {item.Modality} with "
                    #             + f"SOPInstanceUID={item.SOPInstanceUID} to temp_file."
                    #         )
                    #         self.task_queue.put(
                    #             self.Item(
                    #                 item.PatientID,
                    #                 item.StudyInstanceUID,
                    #                 item.SOPClassUID,
                    #                 "",
                    #                 item.Modality,
                    #                 item.SOPInstanceUID,
                    #                 item.Attempt_No + 1,
                    #             )
                    #         )
                    #     else:
                    #         TaskManager.task_logger.info(
                    #             f"Successfully moved {item.Modality} with "
                    #             + f"SOPInstanceUID={item.SOPInstanceUID} to temp_file."
                    #         )
                    # except TypeError as e:
                    #     TaskManager.task_logger.error(
                    #         f"Error putting {item.Modality} to queue: " + f"{e}"
                    #     )
                else:
                    TaskManager.task_logger.error(
                        f"Failed to move {item.Modality} with "
                        + f"SOPInstanceUID={item.SOPInstanceUID} to SCP."
                    )
            except TypeError as e:
                TaskManager.task_logger.info(
                    f"Error putting {item.Modality} to queue: " + f"{e}"
                )
        else:
            TaskManager.task_logger.info(
                f"SOPInstanceUID={item.SOPInstanceUID} already in temp_file"
            )

    def run_record(self, item):
        """_summary_

        Parameters
        ----------
        item : _type_
            _description_
        """
        TaskManager.task_logger.info(
            f"Attempting task for {item.Modality} -- "
            + f"PatientID={item.PatientID}, "
            + f"StudyInstanceUID={item.StudyInstanceUID}, "
            + f"SeriesInstanceUID={item.SeriesInstanceUID}, "
            + f"SOPInstanceUID={item.SOPInstanceUID}, "
            + f"Attempt_No={item.Attempt_No}"
        )
        # Check if it is in temp folder
        status_temp = self.File_Manager.query_uid(
            item.PatientID,
            item.Modality,
            item.StudyInstanceUID,
            item.SOPInstanceUID,
            series_uid=item.SeriesInstanceUID
        )
        if not status_temp:
            status = self.scu.move_dicom_to_scp(
                item.PatientID,
                item.StudyInstanceUID,
                item.SOPClassUID,
                item.SeriesInstanceUID,
                "SERIES",
            )
            self.scp.received_dicom = []

            try:
                if status.status:
                    TaskManager.task_logger.error(
                        f"Failed to move {item.Modality} with "
                        + f"SeriesInstanceUID={item.SeriesInstanceUID} to temp_file."
                    )
                    self.task_queue.put(
                        self.Item(
                            item.PatientID,
                            item.StudyInstanceUID,
                            item.SOPClassUID,
                            item.SeriesInstanceUID,
                            item.Modality,
                            item.SOPInstanceUID,
                            item.Attempt_No + 1,
                        )
                    )
                else:
                    TaskManager.task_logger.info(
                        f"Successfully moved {item.Modality} with "
                        + f"SeriesInstanceUID={item.SeriesInstanceUID} to temp_file."
                    )
            except TypeError as e:
                TaskManager.task_logger.info(
                    f"Error putting {item.Modality} to queue: " + f"{e}"
                )
        else:
            TaskManager.task_logger.info(
                f"SOPInstanceUID={item.SOPInstanceUID} already in temp_file"
            )

    def run_dose(self, item):
        """_summary_

        Parameters
        ----------
        item : _type_
            _description_
        """
        TaskManager.task_logger.info(
            f"Attempting task for {item.Modality} -- "
            + f"PatientID={item.PatientID}, "
            + f"StudyInstanceUID={item.StudyInstanceUID}, "
            + f"SeriesInstanceUID={item.SeriesInstanceUID}, "
            + f"SOPInstanceUID={item.SOPInstanceUID}, "
            + f"Attempt_No={item.Attempt_No}"
        )
        # The RT_Plan Collects the SOPInstanceUID for the RT_Dose,
        # Need to query instance rather than series
        # Check if it is in temp folder
        status_temp = self.File_Manager.query_uid(
            item.PatientID,
            item.Modality,
            item.StudyInstanceUID,
            item.SOPInstanceUID,
        )
        if not status_temp:
            status = self.scu.move_dicom_to_scp(
                item.PatientID,
                item.StudyInstanceUID,
                item.SOPClassUID,
                item.SOPInstanceUID,
                "IMAGE",
            )

            try:
                if status.status:
                    TaskManager.task_logger.error(
                        f"Failed to move {item.Modality} with "
                        + f"SOPInstanceUID={item.SOPInstanceUID} to temp_file."
                    )
                    self.task_queue.put(
                        self.Item(
                            item.PatientID,
                            item.StudyInstanceUID,
                            item.SOPClassUID,
                            item.SeriesInstanceUID,
                            item.Modality,
                            item.SOPInstanceUID,
                            item.Attempt_No + 1,
                        )
                    )
                else:
                    TaskManager.task_logger.info(
                        f"Successfully moved {item.Modality} with "
                        + f"SeriesInstanceUID={item.SeriesInstanceUID} to temp_file."
                    )
            except TypeError as e:
                TaskManager.task_logger.info(
                    f"Error moving {item.Modality} to temp_file. " + f"{e}"
                )
            try:
                ds = self.scp.received_dicom[0]
            except IndexError as e:
                TaskManager.task_logger.error(
                    f"Did not receive {item.Modality} for "
                    + f"PatientID={item.PatientID} with "
                    + f"SOPInstanceUID={item.SOPInstanceUID} "
                    + f"{e}"
                )
                self.task_queue.put(
                    self.Item(
                        item.PatientID,
                        item.StudyInstanceUID,
                        item.SOPClassUID,
                        "",
                        item.Modality,
                        item.SOPInstanceUID,
                        item.Attempt_No + 1,
                    )
                )
                self.scp.received_dicom = []

                return
            self.scp.received_dicom = []

        else:
            TaskManager.task_logger.info(
                f"SOPInstanceUID={item.SOPInstanceUID} already in temp_file"
            )

    def run_image(self, item):
        """
        Run a task for an imaging modality.

        This method handles tasks related to imaging modality. It attempts to move the image
        data to Orthanc, logs the outcome of the operation, and enqueues additional
        tasks if necessary.

        Parameters
        ----------
        item : collections.namedtuple
            A namedtuple representing the task item. It should have attributes including
            'Modality', 'PatientID', 'StudyInstanceUID', 'SeriesInstanceUID',
            'SOPInstanceUID', 'Attempt_No', and optionally 'FrameOfReferenceUID'.

        Notes
        -----
        - If 'SeriesInstanceUID' is empty, the method queries for the CT scan, and if found,
        moves it to the SCP (Service Class Provider). If not found, it logs an error and
        re-enqueues the task.
        - If 'SeriesInstanceUID' is not empty, the method checks if the CT scan is already
        in Orthanc. If not, it queries for any related REG (Registration) data and enqueues
        them. It then moves other DICOM data with the same FrameOfReferenceUID to the SCP
        and enqueues them. Finally, it moves the CT scan to Orthanc and logs the outcome.
        """

        image_info = {
            "PatientID": item.PatientID,
            "StudyUID": item.StudyInstanceUID,
            "SeriesUID": item.SeriesInstanceUID,
            "SOPInstanceUID": item.SOPInstanceUID,
            "Attempt_No": item.Attempt_No,
        }
        msg = f"Attempting task for {item.Modality}."
        TaskManager.task_logger.info(msg, extra=image_info)

        status_temp = self.File_Manager.query_uid(
            item.PatientID,
            item.Modality,
            item.StudyInstanceUID,
            item.SOPInstanceUID,
            series_uid=item.SeriesInstanceUID,
        )
        if not status_temp:
            # First get SeriesInstanceUID, we get this from find_dicom_source if ""
            if item.SeriesInstanceUID == "":
                results = self.scu.query_dicom_rt(
                    item.PatientID,
                    item.StudyInstanceUID,
                    item.SOPInstanceUID,
                    item.SOPClassUID,
                    "",
                )
                if results:
                    TaskManager.task_logger.info(
                        f"Successfully found {item.Modality} with "
                        + f"SeriesInstanceUID={results[0]['SeriesInstanceUID']}"
                    )
                    self.task_queue.put(
                        self.Item(
                            item.PatientID,
                            item.StudyInstanceUID,
                            item.SOPClassUID,
                            results[0]["SeriesInstanceUID"],
                            item.Modality,
                            item.SOPInstanceUID,
                            0,
                        )
                    )
                else:
                    TaskManager.task_logger.error(
                        f"Error querying {item.Modality} with "
                        + f"SeriesInstanceUID={item.SeriesInstanceUID}"
                    )
                    self.task_queue.put(
                        self.Item(
                            item.PatientID,
                            item.StudyInstanceUID,
                            item.SOPClassUID,
                            item.SeriesInstanceUID,
                            item.Modality,
                            item.SOPInstanceUID,
                            item.Attempt_No + 1,
                        )
                    )
            else:
                status = self.scu.move_dicom_to_scp(
                    item.PatientID,
                    item.StudyInstanceUID,
                    item.SOPClassUID,
                    item.SeriesInstanceUID,
                    "SERIES",
                )
                self.scp.received_dicom = []

                try:
                    if status.status:
                        msg = f"Failed to move {item.Modality} to SCP. Status={status[-1][-1]}"
                        TaskManager.task_logger.error(msg, extra=image_info)
                        self.task_queue.put(
                            self.Item(
                                item.PatientID,
                                item.StudyInstanceUID,
                                item.SOPClassUID,
                                item.SeriesInstanceUID,
                                item.Modality,
                                item.SOPInstanceUID,
                                item.Attempt_No + 1,
                            )
                        )
                    else:
                        msg = f"Successfully moved {item.Modality} to SCP."
                        TaskManager.task_logger.info(msg, extra=image_info)
                except TypeError as e:
                    msg = f"Failed to move {item.Modality} to SCP. {e}"
                    TaskManager.task_logger.error(msg, extra=image_info)
                    TaskManager.task_logger.debug(msg, extra=image_info, exc_info=True)
        else:
            msg = f"{item.Modality} already in Temp Directory."
            TaskManager.task_logger.info(msg, extra={**image_info, "Source": None})
