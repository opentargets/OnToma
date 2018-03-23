Welcome to |project_name| documentation!
========================================

OnToma is a python module that helps you map your disease/phenotype terms to the
ontology we use in the Open Targets platform. 


The ontology we use in the Open Targets platform is a subset (aka. _slim_) of 
the EFO ontology _plus_ any HPO terms for which a valid EFO mapping could
not be found.

.. image:: OTontologydiagram.png

This package tries to take the final structure into account and avoids mapping 
to terms that are not currently in the ontology

Contents:

.. toctree::
   :maxdepth: 2

   thereadme
   ontoma
   ols
   zooma
   oxo

Features
--------

- Wrap OLS, OXO, Zooma in a pythonic API
- Tries to find mappings iteratively using the faster methods first
- Checks if mapping is in the subset of EFO that gets included in the 
Open Targets platform
- *tries to* follow the procedure highlighted in https://github.com/opentargets/data_release/wiki/EFO-Ontology-Annotation-Process

.. image:: https://github.com/opentargets/data_release/raw/master/diagram.jpg

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`




