def test_find_term_asthma(ontclient):
    assert ontclient.find_term('asthma') == ['EFO_0000270']


def test_is_included(ontclient):
    assert ontclient.filter_identifiers_by_efo_current(['ORDO:354']) == ['ORDO:354']
    # FIXME: This represents a term which is in EFO, but perhaps should not be considered by OnToma, because this is
    # FIXME: a body part rather than a disease/drug response/etc.
    assert ontclient.filter_identifiers_by_efo_current(['UBERON:0000310']) == ['UBERON:0000310']


def test_find_term_excludes(ontclient):
    assert not ontclient.find_term('breast')


def test_suggest_hp_term_not_excluded(ontclient):
    assert ontclient.find_term('hypogammaglobulinemia') == ['Orphanet_183669']


def test_catch_ordo(ontclient):
    assert ontclient.find_term('Camptodactyly-arthropathy-coxa-vara-pericarditis syndrome') == ['EFO_0009028']
    assert set(ontclient.find_term('OMIM:208250', code=True)) == {'EFO_0009028', 'MONDO_0008828'}


def test_query_comma(ontclient):
    # The test deliberately expects no results, since a match from “3-methylglutaconic aciduria, type III” to
    # “Orphanet_67047” is obtained from fuzzy OLS lookup.
    assert ontclient.find_term('3-methylglutaconic aciduria, type III') == []


def test_find_term_alzheimer(ontclient):
    assert ontclient.find_term('alzheimer\'s disease') == ['EFO_0000249']
