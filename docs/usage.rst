Usage
=====



Installation
------------
.. code-block:: bash
    pip install ontoma



Querying from the Python API
----------------------------
Looking for a disease or phenotype string is simple:
.. code-block:: python
    from ontoma import OnToma

    otmap = OnToma()
    print(otmap.find_term('asthma'))

    #outputs:
    'http://www.ebi.ac.uk/efo/EFO_0000270'

You can obtain more details using the `verbose` flag:
.. code-block:: python
    print(otmap.find_term('asthma',verbose=True))

    #outputs something similar to:
    {'term':'http://www.ebi.ac.uk/efo/EFO_0000270','label':'asthma',
    'source':'EFO OBO','quality':'match' ...}



Querying from the command line
------------------------------

The command line version can be invoked with :code:`ontoma` (type :code:`ontoma --help` to find out about the usage):
.. code-block:: bash
    ontoma <input_file> <output_file>

The input file can be replaced with :code:`-` to read from STDIN and write to STDOUT:
.. code-block:: bash
    echo 'asthma' | ontoma - <output_file>

The output will will contain the result, where it came from and the degree of confidence of the match (one of {match, fuzzy, check}):
.. code-block::
    http://www.ebi.ac.uk/efo/EFO_0000270    asthma  EFO OBO     match

Piping also works for the output. If you want to find the string “foo” from
the results, you can:
.. code-block::
    ontoma <input_file> - | grep "foo"
