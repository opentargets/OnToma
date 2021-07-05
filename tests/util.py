def assert_result_ot_label(ontoma_results, expected_ot_labels):
    assert {result.id_ot_schema for result in ontoma_results} == set(expected_ot_labels)
