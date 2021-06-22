import os

from click.testing import CliRunner

from ontoma.cli import ontoma


def test_help():
    runner = CliRunner()
    result = runner.invoke(ontoma, ['--help'])
    assert result.exit_code == 0


def test_file_batch_input(rootdir):
    runner = CliRunner()
    test_file = os.path.join(rootdir, 'batch_input_test.txt')
    result = runner.invoke(ontoma, [
        '--infile', test_file, '--outfile', '/tmp/ontoma-1.txt', '--cache-dir', '/tmp/efo_cache'
    ])
    assert result.exit_code == 0
    output = open('/tmp/ontoma-1.txt').read()
    assert 'EFO_0000270' in output
    # assert 'MONDO_0002279' in output  # Related synonyms are not processed by this version.
