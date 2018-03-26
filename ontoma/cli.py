import click
import csv
import logging

from ontoma.interface import OnToma

logger = logging.getLogger(__name__)

@click.command()
@click.argument('infile', type=click.File('r'))
@click.argument('outfile', type=click.File('w'))
@click.option('--skip-header', '-s', is_flag=True, default=False)
def ontoma(infile,outfile, skip_header):
    '''Map your input to the ontology used by the Open Targets Platform
    '''
    otmap = OnToma()
    efowriter = csv.writer(outfile, delimiter='\t')

    '''find EFO term'''
    mapped = 0
    filtered = (line.rstrip() for line in infile)
    for i, row in enumerate(filtered):
        if i == 0 and skip_header:
            continue
        efoid = otmap._find_term_from_string(row)
        if efoid[0]:
            mapped +=1
            efowriter.writerow(efoid)
        else:
            efowriter.writerow([''])


    click.echo("Completed. Parsed {} rows. "
                       "Found {} EFOids. "
                       "Skipped {} ".format(i,mapped,i-mapped)
                       )
