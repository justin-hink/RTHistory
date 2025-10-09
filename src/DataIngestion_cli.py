import argparse

def argument_parser():
    parser = argparse.ArgumentParser(
        prog="RTHistory",
        description="""A software package developed for clinics to
        reduce manual labor when collecting a patients radiation
        treatment history.""",
    )

    parser.add_argument(
        "-v", "--verbose", help="Be verbose", action="store_true", dest="logging_verbose"
    )
    parser.add_argument(
        "-d", "--debug", help="Print debugging statements", action="store_true", dest="logging_debug"
    )

    subparsers = parser.add_subparsers(dest="command", required=True, help="Sub-command help")

    # Only 'mrn' subcommand is needed
    parser_mrn = subparsers.add_parser("mrn", help="Process data for a specific patient MRN")
    parser_mrn.add_argument("MRN", help="The MRN of the patient to process")

    # Only 'mrn' subcommand is needed
    parser_config = subparsers.add_parser("config", help="Input AE info.")
    # parser_config.add_argument("config", help="Input AE info.")

    return parser.parse_args()
