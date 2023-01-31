import csv
import logging
from sys import stderr
import os

import click

from ontoma.constants import RESULT_FIELDS, EFO_DEFAULT_VERSION
from ontoma.interface import OnToma

logger = logging.getLogger()


def get_version() -> str:
    """
    It reads the VERSION file in the same directory as the script and returns its contents

    Returns:
      The version number of the script.
    """
    script_path = os.path.dirname(__file__)
    return open(f"{script_path}/../VERSION").read().strip()


@click.command()
@click.option(
    "--infile",
    type=click.File("r"),
    default="/dev/stdin",
    help="File to read the input from, one per line. The type of input is determined by --input-type.",
)
@click.option(
    "--outfile",
    type=click.File("w"),
    default="/dev/stdout",
    help="File to output the mappings to. The format is two column TSV file. First column is your query, and the "
    "second column contains the mappings. When no mappings were found, no lines will be output. When multiple "
    "mappings are found, multiple lines will be output, with one mapping per line.",
)
@click.option(
    "--input-type",
    type=click.Choice(["string", "ontology"]),
    default="string",
    help="Can be one of two values. When set to “string” (default), input values are treated as strings, e.g. "
    "“Diabetes”. When set to “ontology”, input values are treated as ontology identifiers, e.g. “MONDO_0005015”.",
)
@click.option(
    "--cache-dir",
    type=str,
    default=None,
    help="A directory to store EFO cache. Specifying it is not required, but will speed up subsequent OnToma runs.",
)
@click.option(
    "--columns",
    type=str,
    default="query,id_ot_schema",
    help=f'Which columns to output, comma separated. The available options are: {",".join(RESULT_FIELDS)}',
)
@click.option(
    "--efo-release",
    type=str,
    default=EFO_DEFAULT_VERSION,
    help=f"EFO release to use. This must be be either “latest”, or match the specific tag name in their GitHub "
    f"releases, for example v3.31.0. By default, {EFO_DEFAULT_VERSION!r} is used.",
)
@click.option("--version", help="Print version number and exit.", is_flag=True)
@click.option(
    "--log-level",
    type=click.Choice(["INFO", "DEBUG", "WARN", "ERROR"]),
    default="INFO",
    help="Log verbosity level.",
)
def ontoma(
    infile, outfile, input_type, cache_dir, columns, efo_release, version, log_level
):
    """Maps ontology identifiers and strings to EFO, the ontology used by the Open Targets Platform."""
    # Initialize logger:
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if version:
        stderr.write(f"OnToma version {get_version()}.\n")
        return

    if infile.name == "/dev/stdin":
        logger.warning(
            "Reading input from STDIN. If this is not what you wanted, re-run with --help to see usage."
        )

    logger.info("Initialising OnToma main interface.")
    otmap = OnToma(cache_dir=cache_dir, efo_release=efo_release)
    columns = columns.split(",")
    efo_writer = csv.DictWriter(outfile, columns, delimiter="\t")
    efo_writer.writeheader()

    logger.info(f"Treating the input as (type = {input_type}) and mapping to EFO.")
    mapped = 0
    failed = 0
    for line in infile:
        query = line.rstrip()
        results = otmap.find_term(query, code=input_type == "ontology")
        for result in results:
            efo_writer.writerow({c: getattr(result, c) for c in columns})
        if results:
            mapped += 1
        else:
            failed += 1

    logger.info(
        f"Processed {mapped + failed} inputs. Of them, found at least one EFO hit for {mapped}, and failed to find any hits for {failed}."
    )
