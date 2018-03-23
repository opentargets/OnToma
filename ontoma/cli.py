import click
import csv

from ontoma.interface import OnToma

@click.command()
def echo():
    """Example script."""
    click.echo("Hello World! Let's map some EFO codes")

@click.command()
@click.argument('input', type=click.File('rb'), nargs=-1)
@click.argument('output', type=click.File('wb'))
def ontoma(input, output):
    """This script works similar to the Unix `cat` command but it writes
    into a specific file (which could be the standard output as denoted by
    the ``-`` sign).
    \b
    Copy stdin to stdout:
        inout - -
    \b
    Copy foo.txt and bar.txt to stdout:
        inout foo.txt bar.txt -
    \b
    Write stdin into the file foo.txt
        inout - foo.txt
    """
    click.echo(click.style("Hello World! Let's map some EFO codes",fg='red'))
    for f in input:
        while True:
            chunk = f.read(1024)
            if not chunk:
                break
            output.write(chunk)
            output.flush()


@click.argument('inf', type=click.File('rb'), nargs=-1)
@click.argument('outf', type=click.File('wb'))

def ontoma_batch(inf,outf):
    otmap = OnToma()
    efowriter = csv.writer(outf, delimiter='\t')
        
    '''find EFO term'''

    with open(inf) as f:
        for i, item in enumerate(csv.reader(f)):
            efoid = otmap.find_term(item)
            efowriter.writerow(efoid)


    click.echo("Completed. Parsed {} rows. "
                       "Found {} EFOids. " 
                       "Skipped {} ".format(i,i,i)
                       )
