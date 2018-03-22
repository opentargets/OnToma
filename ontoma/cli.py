import click

# @click.command()
# def ontoma():
#     """Example script."""
#     click.echo("Hello World! Let's map some EFO codes")

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