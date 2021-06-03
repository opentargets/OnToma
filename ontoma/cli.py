import click
import csv
import logging

from ontoma.interface import OnToma
from ontoma.constants import FIELDS

logger = logging.getLogger(__name__)


@click.command()
@click.argument('infile', type=click.File('r'))
@click.argument('outfile', type=click.File('w'))
@click.option('--skip-header', '-s', is_flag=True, default=False)
def ontoma(infile, outfile, skip_header):
    '''Map your input to the ontology used by the Open Targets Platform
    '''
    logger.info('Initializing ontoma main interface...')
    otmap = OnToma()
    efowriter = csv.DictWriter(outfile, FIELDS, delimiter='\t')
    efowriter.writeheader()

    '''find EFO term'''
    mapped = 0
    filtered = (line.rstrip() for line in infile)
    for i, row in enumerate(filtered):
        if i == 0 and skip_header:
            continue
        efoid = otmap.find_term(row, verbose=True)
        if efoid:
            mapped +=1
            efoid['query'] = row
            efowriter.writerow(efoid)
        else:
            efowriter.writerow({'query':row})


    click.echo("Completed. Parsed {} rows. "
                       "Found {} EFOids. "
                       "Skipped {} ".format(i+1,mapped,i-mapped+1), err=True
                       )
