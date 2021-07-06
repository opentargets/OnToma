import argparse

from ontoma.interface import OnToma
from ontoma import ontology


def read_mappings(filename):
    return {  # query, term, label
        line.split('\t')
        for line in open(filename).read().splitlines()
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Compares results between two OnToma runs.')
    parser.add_argument('--old', required=True)
    parser.add_argument('--new', required=True)
    args = parser.parse_args()

    ontoma = OnToma(cache_dir='/tmp/efo_cache')
    old_mappings, new_mappings = [set(open(f).read().splitlines()) for f in (args.old, args.new)]
    all_mappings = sorted(old_mappings | new_mappings)

    print('\t'.join(['Query', 'EFO URI', 'EFO label', 'Old', 'New', 'Mapping category']))
    for m in all_mappings:
        query, efo_uri = m.split('\t')
        normalised_id = ontology.normalise_ontology_identifier(efo_uri)

        # Check whether the URI is indeed in EFO
        if ontoma.filter_identifiers_by_efo_current([normalised_id]) == [normalised_id]:
            present_in_efo = True
            efo_label = ontoma.get_label_from_efo(normalised_id)
            mapping_quality = '🟢 Letter to letter' if query.lower() == efo_label.lower() else ''
        else:
            present_in_efo = False
            efo_label = ''
            mapping_quality = '🟥 Not in EFO'

        print('\t'.join([
            query,
            efo_uri,
            efo_label,
            'TRUE' if m in old_mappings else '',
            'TRUE' if m in new_mappings else '',
            mapping_quality
        ]))
