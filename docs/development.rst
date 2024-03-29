Development
===========



Set up environment
------------------

.. code-block:: bash

    git clone https://github.com/opentargets/OnToma.git
    python3 -m venv env
    source env/bin/activate
    pip install --editable .



Install development packages
----------------------------

.. code-block:: bash

    python3 -m pip install --upgrade \
      build pip twine \
      pytest \
      sphinx sphinx-rtd-theme



Installing locally and running the tests
----------------------------------------

.. code-block:: bash

    pip install -e .
    pytest



Adding a dependency
-------------------
Installation dependencies are stored in the ``setup.py`` file, in the ``install_requires`` section.



Releasing a new version
-----------------------
#. Modify the version in the ``VERSION`` file.
#. Add a tag: ``git tag $(cat VERSION) && git push origin --tags``.
#. Create a release on GitHub.
#. Generate distribution archives: ``python3 -m build``.
#. Upload the release: ``python3 -m twine upload dist/*``. Use the usual login and password for PyPi.



Performing a comparison benchmark
---------------------------------
This can be used in case of major updates to OnToma algorithm to make sure they don't break things significantly.

#. Call ``benchmark/sample.sh`` to create a random sample of input strings. This will fetch 100 random records each from ClinVar, PhenoDigm, and gene2phenotype sources. Together they are thought to cover a wide range of use case well. Due to deduplication, the final file ``sample.txt`` may contain slightly less than 300 records.
#. Optionally, to add some additional samples from PanelApp JSON file, you can do: ``jq 'keys[]' diseaseToEfo_results.json | tr -d '"' | shuf -n 100 >> sample.txt; sort -uf -o sample.txt sample.txt``.
#. Process the file so that the final output format is a two column TSV file with the following columns: original query; full URI for an EFO term. Do not include the header.
    * For OnToma pre-v1.0.0, use: ``time ontoma sample.txt - | awk -F$'\t' '{if (length($2) != 0) {print $1 "\t" $2}}' | tail -n+2 > ontoma_old.txt``.
    * For OnToma v1.0.0+, use: ``time ontoma --cache-dir EFO_CACHE --columns query,id_full_uri <sample.txt | tail -n+2 >ontoma_new.txt``.
#. Collate the results by using ``benchmark/collate.py --old ontoma_old.txt --new ontoma_new.txt > benchmark_comparison.txt``. Load the resulting file into Google Sheets and manually mark the mappings as good/bad/uncertain. Exact, letter to letter matches are marked as good automatically.
