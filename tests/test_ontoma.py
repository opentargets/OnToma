
def test_find_term_asthma(ontclient):
    assert ontclient.find_term('asthma') == 'http://www.ebi.ac.uk/efo/EFO_0000270'

def test_is_included(ontclient):
    assert ontclient._is_included('http://www.orpha.net/ORDO/Orphanet_354')
    assert not ontclient._is_included('http://purl.obolibrary.org/obo/UBERON_0000310')

def test_find_term_excludes(ontclient):
    assert not ontclient.find_term('breast')

def test_suggest_hp_term_not_excluded(ontclient):
    assert ontclient.find_term('hypogammaglobulinemia') == 'http://www.orpha.net/ORDO/Orphanet_229720'

def test_catch_ordo(ontclient):
    assert ontclient.find_term('Camptodactyly-arthropathy-coxa-vara-pericarditis syndrome') == 'http://purl.obolibrary.org/obo/MONDO_0008828'
    assert ontclient.find_term('208250') == 'http://www.ebi.ac.uk/efo/EFO_0009028'
    assert ontclient.find_term('208250',suggest=True) == 'http://www.ebi.ac.uk/efo/EFO_0009028'

def test_query_comma(ontclient):
    assert ontclient.find_term('3-methylglutaconic aciduria, type III') == 'http://www.orpha.net/ORDO/Orphanet_67047'

def test_find_term_alzheimer(ontclient):
    assert ontclient.find_term('alzheimer\'s disease') == 'http://www.ebi.ac.uk/efo/EFO_0000249'
