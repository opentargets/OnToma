def test_find_term_asthma(ontclient):
    assert ontclient.find_term('asthma') == 'http://www.ebi.ac.uk/efo/EFO_0000270'


def efo_direct_match(ontclient):
    assert ontclient.find_term('Dementias') == 'http://www.ebi.ac.uk/efo/EFO_0003862'
    assert ontclient.find_term('290.1',code="ICD9CM") == 'http://www.ebi.ac.uk/efo/EFO_0003862'
    
def efo_synonym_match(ontclient):
    assert ontclient.find_term('Prostate cancer') == 'http://www.ebi.ac.uk/efo/EFO_0001663'
    assert ontclient.find_term('185', code="ICD9CM") == 'http://www.ebi.ac.uk/efo/EFO_0001663'


def efo_icd9_match(ontclient):
    assert ontclient.find_term('Other dermatoses') == 'http://www.ebi.ac.uk/efo/EFO_0002496'
    assert ontclient.find_term('702', code="ICD9CM") == 'http://www.ebi.ac.uk/efo/EFO_0002496'


def efo_match_with_apostrophe(ontclient):
    assert ontclient.find_term('Alzheimer\'s disease', '290.11') == 'http://www.ebi.ac.uk/efo/EFO_0000249'
    assert ontclient.find_term('290.11', code="ICD9CM") == 'http://www.ebi.ac.uk/efo/EFO_0000249'

def match_with_hpo(ontclient):
    assert ontclient.find_term('Jaundice') == 'http://www.ebi.ac.uk/efo/EFO_0000952'
    assert ontclient.find_term('573.5', code="ICD9CM") == 'http://www.ebi.ac.uk/efo/EFO_0000952'


def no_efo_match(ontclient):
    #TODO: find another one!
    matched_efos = ontclient.find_term('Candidiasis', '275.1')
    # self.assertEqual(len(matched_efos),0 )