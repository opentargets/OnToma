import argparse


def read_mappings(filename):
    contents = [  # query, term, label
        line.split('\t')
        for line in open(filename).read().splitlines()
    ]
    return {  # query + term → label
        f'{line[0]}\t{line[1]}': line[2]
        for line in contents
    }


parser = argparse.ArgumentParser('Compares results between two OnToma runs.')
parser.add_argument('--old', required=True)
parser.add_argument('--new', required=True)
args = parser.parse_args()

old_mappings, new_mappings = [read_mappings(f) for f in (args.old, args.new)]
all_mappings = sorted(old_mappings.keys() | new_mappings.keys())

print('\t'.join(['Query', 'EFO URI', 'EFO label', 'Present in the old set', 'Present in the new set',
                 'Mapping quality']))
for m in all_mappings:
    query, efo_id = m.split('\t')

    # If possible, obtain the EFO label from new mappings (old ones may be incorrect).
    mapping_label = new_mappings.get(m) or old_mappings.get(m)

    # If query and EFO label are exactly the same, automatically mark the mapping as GOOD.
    mapping_quality = ''
    if query.lower() == mapping_label.lower():
        mapping_quality = 'GOOD'

    print('\t'.join([
        query,
        efo_id,
        mapping_label,
        str(m in old_mappings),
        str(m in new_mappings),
        mapping_quality
    ]))
