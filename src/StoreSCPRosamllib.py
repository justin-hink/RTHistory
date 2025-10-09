from pathlib import Path
from ._globals import TEMP_DIRECTORY
from pydicom.dataset import Dataset
from rosamllib.networking import StoreSCP


class MyStoreSCP(StoreSCP):
    # Class-level variable shared across all instances
    received_dicom = []

    def handle_store(self, event) -> Dataset:
        msg = f"Received {event.dataset.Modality} with SOPInstanceUID={event.dataset.SOPInstanceUID}."
        self.logger.debug(msg)
        try:
            ds = event.dataset
            ds.file_meta = event.file_meta

            # Append to class-level list
            self.__class__.received_dicom.append(ds)

            # Save to TEMP_DIRECTORY
            series_folder = (
                Path(TEMP_DIRECTORY) / ds.PatientID / ds.StudyInstanceUID / ds.Modality / ds.SeriesInstanceUID
            )
            series_folder.mkdir(parents=True, exist_ok=True)
            file_path = series_folder / f"{ds.SOPInstanceUID}.dcm"
            ds.save_as(str(file_path), write_like_original=False)

            self.logger.info(f"Saved DICOM to {file_path}")

            status_ds = Dataset()
            status_ds.Status = 0x0000
            return status_ds

        except Exception as e:
            self.logger.error(f"Error handling C-STORE request: {e}")
            status_ds = Dataset()
            status_ds.Status = 0xC000
            return status_ds
