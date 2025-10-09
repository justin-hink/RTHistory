import os
import sys
import json
import time
import datetime
import logging
from pathlib import Path
from time import strftime, gmtime

from pynetdicom import debug_logger
from pynetdicom.pdu_primitives import SOPClassExtendedNegotiation

from .PdfParser_Rosamllib import run
from .QueryRetrieveSCU_rosamllib import MySCU
from .StoreSCPRosamllib import MyStoreSCP
from .TaskManagerRosamllib import TaskManager
from .config import load_config
from ._globals import LOGS_DIRECTORY, LOG_FORMATTER
from .DataIngestion_cli import argument_parser

# debug_logger()  # Enable if you want detailed DICOM network logs


def setup_core_logger(log_level_cli: str = None):
    """Sets up the main logger for the DataIngestion process."""
    logger_path = os.path.join(LOGS_DIRECTORY, "core")
    os.makedirs(logger_path, exist_ok=True)

    core_logger = logging.getLogger("core")
    core_logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(
        os.path.join(logger_path, f"core_{datetime.date.today().strftime('%Y%m%d')}.log")
    )
    file_handler.setFormatter(LOG_FORMATTER)
    core_logger.addHandler(file_handler)

    if log_level_cli:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(LOG_FORMATTER)
        console_handler.setLevel(log_level_cli)
        core_logger.addHandler(console_handler)

    return core_logger


def start():
    """Main entry point for DataIngestion CLI."""
    args = argument_parser()

    # Determine log verbosity
    if getattr(args, "logging_debug", False):
        log_level_cli = "DEBUG"
    elif getattr(args, "logging_verbose", False):
        log_level_cli = "INFO"
    else:
        log_level_cli = None

    core_logger = setup_core_logger(log_level_cli)
    core_logger.info("Starting DataIngestion ...")
    start_time = time.time()

    # Handle CLI command for config creation
    if "config" in args.command:
        load_config()
        sys.exit(0)

    # --- Load configuration ---
    try:
        config = load_config()
    except Exception as e:
        core_logger.error(f"Failed to load configuration: {e}")
        print("Configuration missing or invalid. Please run with 'config' first.")
        sys.exit(1)

    scp = config["SCP_SERVER"]
    clinical = config["CLINICAL_SERVER"]

    SCP_AETITLE = scp["AETITLE"]
    SCP_HOST = scp["HOST"]
    SCP_PORT = scp["PORT"]

    CLINICAL_AETITLE = clinical["AETITLE"]
    CLINICAL_HOST = clinical["HOST"]
    CLINICAL_PORT = clinical["PORT"]

    try:
        core_logger.info(f"Processing patient with MRN: {args.MRN}")
        core_logger.debug(
            f"SCP AE={SCP_AETITLE}, HOST={SCP_HOST}, PORT={SCP_PORT} | "
            f"Clinical AE={CLINICAL_AETITLE}, HOST={CLINICAL_HOST}, PORT={CLINICAL_PORT}"
        )

        # Initialize DICOM communication objects
        scu = MySCU(SCP_AETITLE)
        scu.add_remote_ae(CLINICAL_AETITLE, CLINICAL_AETITLE, CLINICAL_HOST, CLINICAL_PORT)
        scu.add_remote_ae(SCP_AETITLE, SCP_AETITLE, SCP_HOST, SCP_PORT)
        scu.add_extended_negotiation(SCP_AETITLE, [SOPClassExtendedNegotiation])

        scp = MyStoreSCP(SCP_AETITLE, SCP_HOST, SCP_PORT)
        tm = TaskManager(scu, scp, mrn=args.MRN, log_level_cli=log_level_cli)
        tm.run()

        run(args.MRN)

        elapsed = strftime("%H:%M:%S", gmtime(time.time() - start_time))
        core_logger.info(f"Finished DataIngestion in {elapsed}.")

    except KeyboardInterrupt as key:
        core_logger.error("Interrupted by user.")
        core_logger.debug(key, exc_info=True)
    except Exception as e:
        core_logger.error(f"Unhandled exception occurred: {e}")
        core_logger.debug(e, exc_info=True)
    finally:
        try:
            if "scp" in locals() and hasattr(scp, "stop"):
                scp.stop()
        except Exception as stop_err:
            core_logger.warning(f"Error stopping SCP: {stop_err}")
        elapsed = strftime("%H:%M:%S", gmtime(time.time() - start_time))
        core_logger.info(f"Total runtime: {elapsed}.")


if __name__ == "__main__":
    start()
