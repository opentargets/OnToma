# OnToma


We want:

```python
from ontoma import find_efo

print(find_efo('asthma'))

#outputs:
'EFO:000270'
```

or the command line version

```sh
ontoma -i <input_file> -o <output_dir>
```

where input file is a file of diseases/traits in either codes or text

```txt
ICD9:720
asthma
alzheimer's
DO:124125
```


## TODO:

look into tokenizer
import OMIM logic

memoize/lru_cache the OBO requests
singleton implementation at module level - defer loading
oxo implementation
zooma
