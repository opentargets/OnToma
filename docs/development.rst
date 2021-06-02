Development
===========

Set up environment
------------------
.. code-block:: bash
    git clone https://github.com/opentargets/OnToma.git
    python3 -m venv venv
    source venv/bin/activate
    pip install --editable .

Install development packages
----------------------------
.. code-block:: bash
    python3 -m pip install --upgrade pylint pytest ipython twine sphinx sphinx-autobuild recommonmark sphinx-rtd-theme

How to add a dependency
-----------------------
Installation dependencies are stored in the :code:`setup.py` file, in the :code:`install_requires` section.
