# see http://click.pocoo.org/5/testing/

from click.testing import CliRunner
from ontoma.cli import ontoma
import os

def test_help():
    runner = CliRunner()
    result = runner.invoke(ontoma,['--help'])
    assert result.exit_code == 0

def test_file_batch_input(rootdir):
    runner = CliRunner()
    test_file = os.path.join(rootdir, 'batch_input_test.txt')
    result = runner.invoke(ontoma, [test_file,'-'])
    assert result.exit_code == 0
    assert 'http://www.ebi.ac.uk/efo/EFO_0000270' in result.output
    assert 'http://www.orpha.net/ORDO/Orphanet_309842' in result.output

def test_batch_matching(rootdir):

    stdin = '\n'.join(['asthma',
                       'Iron-metabolism disorder',
                       'Alzheimer'
                       ])

    runner = CliRunner()
    result = runner.invoke(ontoma, args=['-','-'],input=stdin)
    assert result.exit_code == 0
    assert 'http://www.ebi.ac.uk/efo/EFO_0000270' in result.output


