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
