import csv
import logging

import click

from ontoma.constants import RESULT_FIELDS
from ontoma.interface import OnToma

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    '--infile',
    type=click.File('r'),
    default='/dev/stdin',
    help='File to read the input from, one per line. The type of input is determined by --input-type.'
)
@click.option(
    '--outfile',
    type=click.File('w'),
    default='/dev/stdout',
    help='File to output the mappings to. The format is two column TSV file. First column is your query, and the '
         'second column contains the mappings. When no mappings were found, no lines will be output. When multiple '
         'mappings are found, multiple lines will be output, with one mapping per line.'
)
@click.option(
    '--input-type',
    type=click.Choice(['string', 'ontology']),
    default='string',
    help='Can be one of two values. When set to “string”, input values are treated as strings, e.g. “Diabetes”. When '
         'set to “ontology”, input values are treated as ontology identifiers, e.g. “MONDO_0005015”.'
)
@click.option(
    '--cache-dir',
    type=str,
    default=None,
    help='A directory to store EFO cache. Specifying it is not required, but will speed up subsequent OnToma runs.'
)
@click.option(
    '--columns',
    type=str,
    default='query,id_ot_schema',
    help=f'Which columns to output, comma separated. The available options are: {",".join(RESULT_FIELDS)}'
)
def ontoma(infile, outfile, input_type, cache_dir, columns):
    """Maps ontology identifiers and strings to EFO, the ontology used by the Open Targets Platform."""
    if infile.name == '/dev/stdin':
        logger.warning('Reading input from STDIN. If this is not what you wanted, re-run with --help to see usage.')

    logger.info('Initialising OnToma main interface.')
    otmap = OnToma(cache_dir)
    columns = columns.split(',')
    efo_writer = csv.DictWriter(outfile, columns, delimiter='\t')
    efo_writer.writeheader()

    logger.info(f'Treating the input as (type = {input_type}) and mapping to EFO.')
    mapped = 0
    failed = 0
    for line in infile:
        query = line.rstrip()
        results = otmap.find_term(query, code=input_type == 'ontology')
        for result in results:
            efo_writer.writerow({c: getattr(result, c) for c in columns})
        if results:
            mapped += 1
        else:
            failed += 1

    logger.info(f'Processed {mapped + failed} inputs. Of them, found at least one EFO hit for {mapped}, and failed to '
                f'find any hits for {failed}.')
