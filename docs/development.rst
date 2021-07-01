Development
===========



Set up environment
------------------

.. code-block:: bash

    git clone https://github.com/opentargets/OnToma.git
    python3 -m venv env
    source env/bin/activate
    pip install --editable .



Install development packages (optional)
---------------------------------------

.. code-block:: bash

    python3 -m pip install --upgrade pytest sphinx sphinx-rtd-theme



Adding a dependency
-------------------
Installation dependencies are stored in the ``setup.py`` file, in the ``install_requires`` section.



Releasing a new version
-----------------------
#. Modify the version in the ``VERSION`` file.
#. Add a tag: ``git tag $(cat VERSION) && git push origin --tags``.
#. Create a release on GitHub.



Performing a comparison benchmark
---------------------------------
This can be used in case of major updates to OnToma algorithm to make sure they don't break things significantly.

#. Call ``benchmark/sample.sh`` to create a random sample of input strings. This will fetch 200 random records each from ClinVar, PhenoDigm, and gene2phenotype sources. Together they are thought to cover a wide range of use case well. Due to deduplication, the final file ``sample.txt`` may contain slightly less than 600 records.
#. Process the file so that the final output format is a two column TSV file, where column 1 is the original query and column 2 is the mapping.

    * For OnToma pre-v1.0.0, use: ``time ontoma sample.txt - | awk -F$'\t' '{if (length($2) != 0) {print $1 "\t" $2}}' | tail -n+2 > ontoma_old.txt``.
    * For OnToma v1.0.0+, use: ``time ontoma --cache-dir EFO_CACHE <sample.txt >ontoma_new.txt``.
