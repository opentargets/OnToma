"""Constants for URLs and other values."""

# Manual mapping databases.
URLS = {
    'MANUAL_XREF': 'https://raw.githubusercontent.com/opentargets/mappings/master/manual_xref.tsv',
    'MANUAL_STRING': 'https://raw.githubusercontent.com/opentargets/mappings/master/manual_string.tsv',
}

# List of fields to available for result output.
RESULT_FIELDS = ('query', 'id_normalised', 'id_ot_schema', 'id_full_uri', 'label')

EFO_DEFAULT_VERSION = 'latest'
