from ontoma import OnToma

def test_find_efo_asthma():
    t = OnToma()
    assert t.find_efo('asthma') == 'http://www.ebi.ac.uk/efo/EFO_0000270'