"""Constants for URLs and other values."""

# Manual mapping databases.
URLS = {
    'MANUAL_XREF': 'https://raw.githubusercontent.com/opentargets/curation/master/mappings/disease/manual_xref.tsv',
    'MANUAL_STRING': 'https://raw.githubusercontent.com/opentargets/curation/master/mappings/disease/manual_string.tsv',
}

# List of fields to available for result output.
RESULT_FIELDS = ('query', 'id_normalised', 'id_ot_schema', 'id_full_uri', 'label')

EFO_DEFAULT_VERSION = 'latest'
