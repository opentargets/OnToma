def test_find_term_asthma(ontclient):
    assert ontclient.find_term('asthma') == 'http://www.ebi.ac.uk/efo/EFO_0000270'

def test_efo_direct_match(ontclient):
    assert ontclient.find_term('Dementias') == 'http://www.ebi.ac.uk/efo/EFO_0004718'

def test_otzooma_mappings_whitespace(ontclient):
    assert ontclient.find_term('Prostate cancer') == 'http://purl.obolibrary.org/obo/MONDO_0008315'

def test_efo_match_with_apostrophe(ontclient):
    assert ontclient.find_term('Alzheimer\'s disease') == 'http://www.ebi.ac.uk/efo/EFO_0000249'

def test_match_with_hpo(ontclient):
    assert ontclient.find_term('Jaundice') == 'http://purl.obolibrary.org/obo/HP_0000952'

def test_exists_in_zooma_but_not_included_in_ot(ontclient):
    #Zooma finds a mapping, but the _is_included could fail
    assert ontclient.find_term('Failure to thrive') == 'http://purl.obolibrary.org/obo/HP_0001508'