from util import assert_result_ot_label


def test_find_term_asthma(ontclient):
    assert_result_ot_label(
        ontclient.find_term('asthma'),
        ['MONDO_0004979']
    )


def test_is_included(ontclient):
    assert ontclient.filter_identifiers_by_efo_current(['MONDO:0018149']) == ['MONDO:0018149']


def test_suggest_hp_term_not_excluded(ontclient):
    assert_result_ot_label(
        ontclient.find_term('hypogammaglobulinemia'),
        ['MONDO_0015977']
    )


def test_catch_ordo(ontclient):
    assert_result_ot_label(
        ontclient.find_term('Camptodactyly-arthropathy-coxa-vara-pericarditis syndrome'),
        ['EFO_0009028']
    )
    assert_result_ot_label(
        ontclient.find_term('OMIM:208250', code=True),
        {'EFO_0009028'}
    )


def test_query_comma(ontclient):
    # The test deliberately expects no results, since a match from “3-methylglutaconic aciduria, type III” to
    # “Orphanet_67047” is obtained from fuzzy OLS lookup.
    assert not ontclient.find_term('3-methylglutaconic aciduria, type III')


def test_find_term_alzheimer(ontclient):
    assert_result_ot_label(
        ontclient.find_term('alzheimer\'s disease'),
        ['MONDO_0004975']
    )
