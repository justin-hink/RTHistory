# src/main_sqlalchemy_logging.py

import sys
import argparse
import time
from pathlib import Path
from .config import load_config
from .QueryRetrieveSCU_rosamllib import MySCU
from .StoreSCPRosamllib import MyStoreSCP
from .TaskManagerRosamllib import TaskManager
from .PdfParser_Rosamllib import run
from .logger_setup import core_logger, TaskManager_task_logger  # SQLAlchemy loggers

def start():
    start_time = time.time()
    config = load_config()
    mrn = input("Please input the PatientID: ")

    core_logger.info(f"Starting DataIngestion for MRN: {mrn}")

    # --- Extract SCP / Clinical info ---
    scp_cfg = config["SCP_SERVER"]
    clinical_cfg = config["CLINICAL_SERVER"]

    SCP_AETITLE = scp_cfg["AETITLE"]
    SCP_HOST = scp_cfg["HOST"]
    SCP_PORT = scp_cfg["PORT"]

    CLINICAL_AETITLE = clinical_cfg["AETITLE"]
    CLINICAL_HOST = clinical_cfg["HOST"]
    CLINICAL_PORT = clinical_cfg["PORT"]

    try:
        # --- Initialize DICOM SCU ---
        scu = MySCU(SCP_AETITLE)
        scu.add_remote_ae(CLINICAL_AETITLE, CLINICAL_AETITLE, CLINICAL_HOST, CLINICAL_PORT)
        scu.add_remote_ae(SCP_AETITLE, SCP_AETITLE, SCP_HOST, SCP_PORT)
        scp = MyStoreSCP(SCP_AETITLE, SCP_HOST, SCP_PORT)

        # --- Run TaskManager ---
        tm = TaskManager(scu, scp, mrn=mrn, log_level_cli="INFO")
        TaskManager.task_logger = TaskManager_task_logger  # assign SQLAlchemy logger
        tm.run()

        # --- Run PDF parser ---
        run(mrn)

        core_logger.info("DataIngestion complete.")
    except Exception as e:
        core_logger.error(f"Unhandled exception: {e}", exc_info=True)
        scp.stop()
    finally:
        scp.stop()
        core_logger.info(f"Finished in {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    start()
