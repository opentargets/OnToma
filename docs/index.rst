OnToma documentation
====================



Introduction
------------
OnToma is a Python module which maps the disease or phenotype terms to `EFO, <https://www.ebi.ac.uk/efo/>`_ the ontology used in the Open Targets platform.

.. note::
    More precisely, only a subset of EFO is used in the Open Targets platform, called *EFO slim.* It is released alongside with every new version of EFO.

OnToma supports two kinds of inputs: either identifiers from other ontologies (e.g. ``OMIM:102900``), or strings (e.g. ``pyruvate kinase hyperactivity``).

The way OnToma operates is by trying a series of lookups in EFO and also querying tools from the EBI ontology stack such as OxO and ZOOMA.

For each input, it will return anywhere from 0 to multiple matches in EFO. This version of OnToma only returns mappings which are considered of high quality.

For each input type, you can run OnToma from command line or directly from other Python code.



Installing OnToma
-----------------
.. code-block:: bash

  pip install ontoma



Running OnToma from CLI
-----------------------
In this mode, the input is a file or STDIN, with one input per line:

.. code-block:: bash

  echo -e 'asthma\npyruvate kinase hyperactivity' | ontoma --input-type string
  echo -e 'OMIM:102900\nOMIM:104310' | ontoma --input-type ontology

The output format is a two column TSV file: the first column is your original query, and the second one is the result. In case no results were found, the query will be missing from the output. In case multiple results were found, the query will appear multiple times in the output.

You can read about additional flags by running ``ontoma --help``.



Running OnToma from Python code
-------------------------------

.. code-block:: python

  from ontoma import OnToma
  otmap = OnToma()
  result = otmap.find_term('asthma')
  result = otmap.find_term('OMIM:102900', code=True)



Speeding up subsequent OnToma runs
----------------------------------
When you initialise an OnToma client, it needs to download and parse the latest EFO OT slim release. Depending on your internet connection, it may take anywhere from 10 seconds to a few minutes.

To speed up subsequent OnToma runs, you can specify a cache directory to avoid doing this every time. This can be supplied by a ``--cache-dir`` option in the CLI or by a ``cache_dir`` parameter in the Python interface.



Contents
--------
.. toctree::
   :maxdepth: 2

   development
   ontoma
   zooma
   oxo