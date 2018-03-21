
def test_find_efo_asthma(ontclient):
    assert ontclient.find_efo('asthma') == 'http://www.ebi.ac.uk/efo/EFO_0000270'