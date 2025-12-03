import os
import pandas as pd
import sys
from tabulate import tabulate
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import letter
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    Image,
    PageBreak,
)
from rosamllib.dicoms import RTDose
from rosamllib.readers import DICOMLoader
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from ._globals import TEMP_DIRECTORY, OUTPUT_DIRECTORY

# stuff i added to try to make images
import io
import numpy as np
import SimpleITK as sitk
import matplotlib

from reportlab.pdfgen import canvas
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import zipfile
import shutil
from .logger_setup import pdf_logger

class PDF_Parser:
    """ """

    def __init__(self, mrn):

        self.mrn = mrn
        self.year = 0
        self.plans = []
        self.timeline_list_year = []
        self.timeline_list_date = []
        self.timeline_list_label = []
        self.timeline_list_pres = []
        self.timeline_list_dose_per_fraction = []
        self.timeline_list_frac_complete = []
        self.table_answers_list = []
        self.table_title_list = []
        self.log_level_cli = None
        self.OAR_Flag = False
        self.pdf_filename = os.path.join(
            TEMP_DIRECTORY, os.path.join(self.mrn, "Radiotherapy_Treatment_History.pdf")
        )
        self.ct_list = []


    @staticmethod
    def resamp(image: sitk.Image, new_spacing=(1.5, 1.5, 1.5), interpolator=sitk.sitkLinear):
        """
        Resample a SimpleITK image to isotropic spacing.
        
        Parameters
        ----------
        image : sitk.Image
            Input SimpleITK image.
        new_spacing : tuple of float
            Desired voxel spacing (isotropic if all elements equal).
        interpolator : sitk.InterpolatorEnum
            Interpolation method (sitk.sitkLinear for CT, sitk.sitkNearestNeighbor for masks).
        
        Returns
        -------
        sitk.Image
            Resampled image.
        """
        try:
            # Original spacing and size
            original_spacing = image.GetSpacing()
            original_size = image.GetSize()

            # Compute new size (preserve physical size of image)
            new_size = [
                int(round(osz * ospc / nspc))
                for osz, ospc, nspc in zip(original_size, original_spacing, new_spacing)
            ]

            # Define resampler
            resample = sitk.ResampleImageFilter()
            resample.SetInterpolator(interpolator)
            resample.SetOutputSpacing(new_spacing)
            resample.SetSize(new_size)
            resample.SetOutputDirection(image.GetDirection())
            resample.SetOutputOrigin(image.GetOrigin())
            resample.SetDefaultPixelValue(image.GetPixelIDValue())  # fill background if needed

            return resample.Execute(image)
        except Exception as e:
            pdf_logger.error(f"Error during resampling: {e}")
            raise


    def create_image(
            self, ct, dose_image=None, aspect_ratio=(16, 18)
        ):


        resamp_ct = PDF_Parser.resamp(ct)
        ct_array = sitk.GetArrayFromImage(resamp_ct).astype(np.float16)

        if dose_image:
            res_dose = dose_image.resample_dose_to_image_grid(ct)
            resampled_dose = PDF_Parser.resamp(res_dose)
            resampled_dose = RTDose(resampled_dose)
            resampled_dose.DoseGridScaling = dose_image.dose_grid_scaling
            resampled_dose.DoseUnits = dose_image.DoseUnits
            dose_array = resampled_dose.get_dose_array().astype(np.float32)
            # res_dose = dose_image.resample_dose_to_image_grid(ct)
            # dose_array = res_dose.get_dose_array() * 100
            coronal_dose = np.sum(dose_array, axis=1)
            sagittal_dose = np.sum(dose_array, axis=2)
            corhalfmax = np.amax(coronal_dose) * 0.1
            saghalfmax = np.amax(sagittal_dose) * 0.1

        aspect_ratio_value = aspect_ratio[0] / aspect_ratio[1]
        fig_width = 8  # Fixed width in inches
        fig_height = fig_width / aspect_ratio_value
        fig, (ax1, ax2) = plt.subplots(
            2,
            1,  # 2 rows, 1 column
            figsize=(fig_width, fig_height),
            gridspec_kw={"height_ratios": [1, 1]},
        )
        fig.tight_layout()
        # apply windowing and leveling for the image
        window_level = 50
        window_width = 400
        min_val = window_level - (window_width / 2)
        max_val = window_level + (window_width / 2)
        windowed_img = np.clip(ct_array, min_val, max_val)
        coronal_img = np.sum(windowed_img, axis=1)
        sagittal_img = np.sum(windowed_img, axis=2)

        # Coronal (X–Z plane)
        ax1.imshow(coronal_img, "gray",  origin="lower")
        ax1.axis("off")
        if dose_image:
            ax1.imshow(
                coronal_dose,
                "jet",
                alpha=0.25 * (coronal_dose > corhalfmax),
                origin="lower",
            )

        # Sagittal (Y–Z plane)
        ax2.imshow(sagittal_img, "gray", origin='lower')
        ax2.axis("off")
        if dose_image:
            ax2.imshow(
                sagittal_dose,
                "jet",
                alpha=0.25 * (sagittal_dose > saghalfmax),
                origin="lower",
            )

        plt.subplots_adjust(hspace=0)
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=300, bbox_inches="tight", pad_inches=0)
        buf.seek(0)
        return Image(
            buf, fig_width * inch / 3.5, fig_height * inch / 3.5, hAlign="LEFT"
        )

    def table_append(
        self,
        plan_row,
    ):
        if "." in str(plan_row["NumberOfFractionsPlanned"]):
            denominator_fraction = str(plan_row["NumberOfFractionsPlanned"]).split(".")[
                0
            ]
        else:
            denominator_fraction = str(plan_row["NumberOfFractionsPlanned"])
        fxcount = len(self.seen_fraction_numbers)
        ratio = str(fxcount) + "/" + str(denominator_fraction)
        # Timeline is top of page
        self.timeline_list_year.append(str(self.study_year))
        self.timeline_list_date.append(str(self.convert_date(self.records[0][1])))
        self.timeline_list_label.append(str(plan_row["RTPlanLabel"]))
        target_dose_list = []
        # Collect all doses from plan_row
        doses = plan_row["PrescriptionDose"]

        max_val = None
        max_marker = ""

        for dose in doses:
            if isinstance(dose, tuple):
                val, marker = dose
            else:
                val, marker = dose, ""

            # Check if this is the new maximum
            if max_val is None or val > max_val:
                max_val = val
                max_marker = marker
        prescription_dose = ""
        dose_per_fraction = ""
        if max_val is not None:
            # Save max dose with marker (if OAR it will keep the *)
            prescription_dose = f"{np.round(max_val, 3)}{max_marker} Gy"
            self.timeline_list_pres.append(prescription_dose)
            dose_per_fraction = f"{np.round(max_val, 3) / float(denominator_fraction)}"
            self.timeline_list_dose_per_fraction.append(dose_per_fraction)
        else:
            prescription_dose = "N/A"
            dose_per_fraction = "N/A"
            self.timeline_list_pres.append(prescription_dose)
            self.timeline_list_dose_per_fraction.append(dose_per_fraction)
        if max_marker:
            self.OAR_Flag = True
        # Fractions completed stays as before
        self.timeline_list_frac_complete.append(
            ratio,
        )
        # Table is each section starting from page 2
        table_title = [
            ["Plan Label:"],
            ["Prescribed Dose:"],
            ["Dose Per Fraction:"],
            ["Prescribed Fractions:"],
        ]
        table_answers = [
            [str(plan_row["RTPlanLabel"])],
            [prescription_dose],
            [dose_per_fraction],
            [str(plan_row["NumberOfFractionsPlanned"])],
        ]

        if self.records:
            # print(self.records[:])
            table_title.append(["Treatment Start:"])
            table_title.append(["Treatment End:"])
            table_title.append(["Fractions Delivered:"])
            table_answers.append([self.convert_date(self.records[0][1])])
            table_answers.append([self.convert_date(self.records[-1][1])])
            table_answers.append(
                [
                    str(fxcount) + " / " + denominator_fraction,
                ]
            )
        return table_title, table_answers

    def create_timeline_table(
        self,
    ):
        timeline_max_widths = [40, 120, 120, 60, 70]
        table_data = [
            [
                "Year",
                "Treatment Start Date",
                "Plan Label",
                "Rx Dose",
                "Dose per Fraction",
                "Fractions Completed",
            ]
        ]  # Start with headers as the first row

        # Append data rows to the table
        for i in range(len(self.timeline_list_year)):
            row = [
                self.timeline_list_year[i],
                self.timeline_list_date[i],
                self.timeline_list_label[i],
                self.timeline_list_pres[i],
                self.timeline_list_dose_per_fraction[i],
                self.timeline_list_frac_complete[i],
            ]
            table_data.append(row)
        # Convert each cell to a Paragraph object with the specified style
        table_data_paragraphs = []
        cell_style = ParagraphStyle(
            name="TableCellStyle", fontname="Helvetica", fontsize=10, leading=12
        )
        for row in table_data:
            row_paragraphs = [Paragraph(str(cell), cell_style) for cell in row]
            table_data_paragraphs.append(row_paragraphs)
        table = Table(
            table_data_paragraphs,
            colWidths=timeline_max_widths,
            style=[
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("LINEBELOW", (0, 0), (-1, 0), 1, colors.black),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.black),
                ("BOX", (0, 0), (-1, -1), 1, colors.black),
                ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.lightgrey, colors.white]),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ],
            hAlign="LEFT",
        )
        return table

    def parse_csv_for_plan(self, rtplans, loader):
        pdf_logger.info("Parsing RTPLAN CSV data.")
        plans = []
        for indx, row in rtplans.iterrows():
            try:
                rtplan = loader.read_instance(row["SOPInstanceUID"])
                inst = loader.get_instance(row["SOPInstanceUID"])
                results_inst, _ = loader.advanced_query(
                    "INSTANCE",
                    dcm_filters={
                        "Modality": "RTRECORD",
                        "ReferencedRTPlanSequence[0].ReferencedSOPInstanceUID": inst.SOPInstanceUID,
                        "TreatmentDate": "*",
                    },
                    return_instances=True,
                )
                treatment_date_list = [
                    int(loader.read_instance(records.SOPInstanceUID).TreatmentDate)
                    for records in results_inst
                ]
                plans.append((rtplan.SOPInstanceUID, min(treatment_date_list)))
            except Exception as e:
                pdf_logger.warning(f"Skipping invalid RTPLAN entry: {e}")
        plans.sort(key=lambda x: float(x[1]))
        return plans

    def convert_date(self, date_str):
        # Define a dictionary mapping month numbers to month names
        date_str = str(date_str)
        month_names = {
            "01": "January",
            "02": "February",
            "03": "March",
            "04": "April",
            "05": "May",
            "06": "June",
            "07": "July",
            "08": "August",
            "09": "September",
            "10": "October",
            "11": "November",
            "12": "December",
        }
        if " " in date_str:
            date_str = date_str.split(" ")[0]
        if "." in date_str:
            date_str = date_str.split(".")[0]
        if "-" in date_str:
            year, month, day = date_str.split("-")
            month = month_names[month]
        else:
            year = str(date_str)[:4]
            month = month_names[str(date_str)[4:6]]
            day = str(date_str)[6:]

        # Assemble the converted date string
        converted_date = f"{month} {day}, {year}"

        return converted_date

    def record_loop(self, plan, loader: DICOMLoader):
        pdf_logger.debug(f"Collecting RTRECORD instances for plan {plan.SOPInstanceUID}.")
        self.records = []
        plan_inst = loader.get_instance(plan.SOPInstanceUID)
        ref_records = loader.get_referencing_nodes(plan_inst, "RTRECORD", "INSTANCE")
        for record_inst in ref_records:
            record = loader.read_instance(record_inst.SOPInstanceUID)
            fraction_number = str(record.TreatmentSessionBeamSequence[0].CurrentFractionNumber)
            if fraction_number not in ("0", "0.0"):
                self.seen_fraction_numbers.add(fraction_number)
            self.records.append(
                [fraction_number, str(record.TreatmentDate), str(record.SOPInstanceUID)]
            )



    def create_summary_table(self, content):
        max_widths = [110, 110, 200]
        # Create the table with fixed column widths
        cell_style = ParagraphStyle(
            name="TableCellStyle", fontname="Helvetica", fontsize=10, leading=12
        )
        # table_title = [[Paragraph(str(item), cell_style) for item in row] for row in table_title]
        for _, (table_title, table_answers, ct_image) in enumerate(
            zip(self.table_title_list, self.table_answers_list, self.ct_list)
        ):
            table_title = [
                [(Paragraph(str(item), cell_style)) for item in row]
                for row in table_title
            ]
            table_answers = [
                [(Paragraph(str(item), cell_style)) for item in row]
                for row in table_answers
            ]
            table_full = Table(
                [
                    [
                        table_title,
                        table_answers,
                        ct_image,
                    ],
                ],
                colWidths=max_widths,
                style=[
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("LINEBELOW", (0, 0), (-1, 0), 1, colors.black),
                    ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.black),
                    ("BOX", (0, 0), (-1, -1), 1, colors.black),
                    (
                        "ROWBACKGROUNDS",
                        (0, 0),
                        (-1, -1),
                        [colors.lightgrey, colors.white],
                    ),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    (
                        "ALIGN",
                        (0, 0),
                        (-1, -1),
                        "LEFT",
                    ),  # Align all cells to the left by default
                    ("ALIGN", (2, 0), (2, -1), "RIGHT"),
                ],
                hAlign="LEFT",
            )
            content.append(table_full)
            # Calculate the maximum width of each column

            # content.append(Spacer(1, 12))
            # content.append(ct_list[count])
            content.append(Spacer(1, 12))

            # Check if the y-coordinate is less than a threshold, create a new page
            # if y_coordinate < 250:
            #     # Add some space before new page
            #     content.append(Spacer(1, 12))
            #     content.append(PageBreak())  # Add page break
            #     y_coordinate = 750  # Reset y-coordinate

        return

    def generate_pdf(self, mrn):
        pdf_logger.info(f"Starting PDF generation for MRN={mrn}")
        try:
            loader = DICOMLoader(TEMP_DIRECTORY/mrn)
            tags_to_index = ["TreatmentDate"]
            loader.load()
            results_inst, results_df = loader.advanced_query("INSTANCE", dcm_filters={"DoseSummationType":"BEAM"}, return_instances=True)
            for dose_beam in results_inst:
                file_path = dose_beam.FilePath
                os.remove(file_path)
            loader = DICOMLoader(TEMP_DIRECTORY/mrn)
            loader.load()
            rtplans = loader.query("INSTANCE", Modality="RTPLAN")
            
            content = []
            styles = getSampleStyleSheet()
            content.append(
                Paragraph(
                    "External Beam Radiotherapy Treatment History", styles["Heading1"]
                )
            )
            content.append(Spacer(1, 12))
            doc = SimpleDocTemplate(self.pdf_filename, pagesize=letter)
            patient_info = loader.read_instance(rtplans["SOPInstanceUID"][0])
            # y_coordinate = 800  # Initial y-coordinate for writing text
            try:
                normal_style = styles["Normal"]
                normal_style.maxWidth = 500  # Adjust the value as needed
                
                
                content.append(
                    Paragraph(
                        f"<b>Patient Name:</b> {patient_info.PatientName}",
                        styles["Heading2"],
                    )
                )
                content.append(
                    Paragraph(
                        f"<b>Date of Birth:</b> {self.convert_date(patient_info.PatientBirthDate)}",
                        styles["Heading2"],
                    )
                )
            except Exception as e:
                msg = f"Could not grab info for the pdf header {str(e)}"
                print(msg)
            # content.append(Spacer(1, 12))
            plans = self.parse_csv_for_plan(rtplans, loader)
            for sop, _ in plans:
                plan = loader.read_instance(sop)
                inst = loader.get_instance(sop)
                # print(f"Finding Stuff for {plan_row['SOPInstanceUID']}")
                self.seen_fraction_numbers = set()  # Set to store seen fraction numbers
                self.record_loop(plan, loader)

                self.records.sort(key=lambda x: x[1])
                if str(self.records[0][1])[0:4] != self.year:
                    self.study_year = str(self.records[0][1])[0:4]
                    self.year = self.study_year
                    content.append(Spacer(1, 12))
                else:
                    self.study_year = ""
                    # Table is each section starting from page 2
                plan_row = {}
                target_rx_dose = []
                plan_row["RTPlanLabel"] = plan.RTPlanLabel
                for dose_ref in plan.DoseReferenceSequence:
                    if hasattr(dose_ref, "TargetPrescriptionDose"):
                        target_rx_dose.append(float(dose_ref.TargetPrescriptionDose))
                    elif hasattr(dose_ref, "OrganAtRiskMaximumDose"):
                        # Store as tuple (value, marker) so formatting later is cleaner
                        target_rx_dose.append((float(dose_ref.OrganAtRiskMaximumDose), "*"))

                plan_row["PrescriptionDose"] = target_rx_dose
                plan_row["NumberOfFractionsPlanned"] = plan.FractionGroupSequence[0].NumberOfFractionsPlanned
                # Make the lists for the tables
                table_title, table_answers = self.table_append(
                    plan_row,
                )
                self.table_title_list.append(table_title)
                self.table_answers_list.append(table_answers)

                # Find the correct RTStruct
                referenced_ct = loader.get_referenced_nodes(inst, "CT", "SERIES", recursive=True)
                referenced_dose = loader.get_referencing_items(inst, "RTDOSE", "INSTANCE")
                ct_skip = False
                if hasattr(plan.BeamSequence[0], "TreatmentMachineName"):
                    if "ViewRay" in plan.BeamSequence[0].TreatmentMachineName:
                        referenced_ct = []
                        self.ct_list.append(
                            Paragraph("Planned in ViewRay. Manually exported.")
                        )
                        ct_skip=True
                    if "TomoTherapy" in plan.BeamSequence[0].TreatmentMachineName:
                        referenced_ct = []
                        self.ct_list.append(
                            Paragraph("Planned in TomoTherapy. Manually exported.")
                        )
                        ct_skip=True
                        
                if hasattr(plan.BeamSequence[0], "RadiationType"):
                    if "electron" in str(plan.BeamSequence[0].RadiationType).lower():
                        if not referenced_dose:
                            referenced_ct = []
                            self.ct_list.append(
                            Paragraph(
                                "Plan dose scaled for electron beam. Manually exported."
                            )
                        )
                        ct_skip=True
                if hasattr(plan, "Manufacturer"):
                    if "siemens" in str(plan.Manufacturer).lower():
                        self.ct_list.append(
                            Paragraph("Decommissioned machine. Maybe manually exportable.")
                        )
                        referenced_ct = []
                        ct_skip=True
                if hasattr(plan.BeamSequence[0], "Manufacturer"):
                    if "siemens" in str(plan.BeamSequence[0].Manufacturer).lower():
                        self.ct_list.append(
                            Paragraph("Decommissioned machine. Maybe manually exportable.")
                        )
                        referenced_ct = []
                        ct_skip=True
                if not ct_skip:
                    if referenced_ct:
                        
                        ct = loader.read_series(referenced_ct[0].SeriesInstanceUID)[0]
                        if referenced_dose:
                            dose = loader.read_instance(referenced_dose[0].SOPInstanceUID)
                            
                            self.ct_list.append(
                                self.create_image(
                                    ct,
                                    dose_image = dose,
                                )
                            )
                        else:
                            self.ct_list.append(self.create_image(ct))
                    else:
                        
                        print(
                            f"Manually export files for \n{plan.SOPInstanceUID}"
                        )
                        pdf_logger.warning(f"Missing CT for plan {plan.SOPInstanceUID}")
                        sys.exit()

            # Create the timeline table, add it to doc
            content.append(self.create_timeline_table())
            if self.OAR_Flag:
                # Add footnote explaining the asterisk
                footnote_text = Paragraph(
                    '<font size=8>* = OrganAtRiskMaximumDose used instead of TargetPrescriptionDose</font>',
                    styles["Normal"],
                )
                content.append(Spacer(1, 6))
                content.append(footnote_text)
            # content.append(Spacer(1, 12))
            content.append(PageBreak())
            # y_coordinate = 750
            # Create the summary table for each plan
            content.append(self.create_summary_table(content))
            

            # Build the PDF
            doc.build(content)
            # doc.build(content, onLaterPages=self.add_footer)

            # Save the PDF
            pdf_logger.info(f"PDF successfully created for MRN={mrn}")
            return
        except Exception as e:
            pdf_logger.error(f"Error generating PDF: {e}")
            raise


    def zip_and_remove_directory(self, directory_path, zip_file_path):
        pdf_logger.debug(f"Zipping output directory: {directory_path}")
        with zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(directory_path):
                for file in files:
                    zipf.write(
                        os.path.join(root, file),
                        os.path.relpath(os.path.join(root, file), directory_path),
                    )
        shutil.rmtree(directory_path)
        pdf_logger.info(f"Created zip archive: {zip_file_path}")


def run(mrn):
    mrn = str(mrn)
    pdf_logger.info(f"Running PDF generator for MRN={mrn}")
    pdf_parser = PDF_Parser(mrn)
    pdf_parser.generate_pdf(mrn)
    directory_to_zip = os.path.join(TEMP_DIRECTORY, mrn)
    if not os.path.exists(OUTPUT_DIRECTORY):
        os.mkdir(OUTPUT_DIRECTORY)
    zip_file_path = os.path.join(OUTPUT_DIRECTORY, mrn + ".zip")
    pdf_parser.zip_and_remove_directory(directory_to_zip, zip_file_path)


def main():
    mrn = str(input("Input MRN: "))
    pdf_logger.info(f"Manual run for MRN={mrn}")
    run(mrn)


if __name__ == "__main__":
    main()
