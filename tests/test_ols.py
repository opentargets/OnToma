
def test_ols_bytype_is_not_ontology(ontclient):
    '''this OLS search would return the efo ontology itself
    '''
    term = ontclient._ols.besthit('Persistent mental disorders due to other conditions',
                                  ontology=['efo'],
                                  field_list=['iri'],
                                  bytype='class')
    assert term is not 'http://www.ebi.ac.uk/efo/efo.owl'
