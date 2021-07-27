from util import assert_result_ot_label


def test_find_term_asthma(ontclient):
    assert_result_ot_label(
        ontclient.find_term('asthma'),
        ['EFO_0000270']
    )


def test_efo_direct_match(ontclient):
    # The test deliberately expects no results, since a match from “Dementias” to “EFO_0004718” is a fuzzy one and
    # cannot be assumed of high quality.
    assert not ontclient.find_term('Dementias')


def test_otzooma_mappings_whitespace(ontclient):
    assert_result_ot_label(
        ontclient.find_term('Prostate cancer'),
        ['MONDO_0008315']
    )


def test_efo_match_with_apostrophe(ontclient):
    assert_result_ot_label(
        ontclient.find_term('Alzheimer\'s disease'),
        ['EFO_0000249']
    )


def test_match_with_hpo(ontclient):
    assert_result_ot_label(
        ontclient.find_term('Jaundice'),
        ['HP_0000952']
    )


def test_exists_in_zooma_but_not_included_in_ot(ontclient):
    # Zooma finds a mapping, but the EFO inclusion check could fail
    assert_result_ot_label(
        ontclient.find_term('Failure to thrive'),
        ['HP_0001508']
    )
