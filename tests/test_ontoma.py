
def test_find_term_asthma(ontclient):
    assert ontclient.find_term('asthma') == 'http://www.ebi.ac.uk/efo/EFO_0000270'