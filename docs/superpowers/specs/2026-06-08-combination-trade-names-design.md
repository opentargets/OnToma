# Combination-product trade names in `drug.py`

**Date:** 2026-06-08
**Branch:** `feat/combination-trade-names`
**Status:** Approved design

## Problem

The Open Targets drug index (`drug_molecule`) encodes combination-product
membership inside the `tradeNames` (and a handful of `synonyms`) of each
component molecule, using the phrase `{molecule} component of {product}`.

For example, Ivacaftor (`CHEMBL2010601`) has:

```
tradeNames: ['Ivacaftor component of orkambi',
             'Ivacaftor component of symkevi',
             'Ivacaftor component of trikafta',
             'Kalydeco']
synonyms:   ['Ivacaftor', 'VX-770']
```

Today OnToma emits each of these strings verbatim as a label. The phrase
`"ivacaftor component of symkevi"` is never a useful query label, while the
combination product name it contains — `symkevi` — is never extracted. As a
result a query for `symkevi` does not resolve to its component molecules.

## Goal

Parse `{molecule} component of {product}` strings and emit
`product -> moleculeId` label records, so that a query for a combination trade
name (`symkevi`) resolves to the component molecules
(`CHEMBL2010601` Ivacaftor and `CHEMBL3544914` Tezacaftor).

## Impact (measured on `gs://open-targets-data-releases/26.03/output/drug_molecule`)

| Metric | Count |
|---|---|
| Raw `component of` strings (`tradeNames` + `synonyms`) | 2,194 |
| Distinct `(product -> id)` pairs | 2,167 |
| Distinct products | 1,124 |
| Products **not** already a label elsewhere (genuinely new) | 962 |
| New `(product -> id)` pairs | 1,875 |
| Products mapping to >1 id (true combinations) | 797 |

Worked example: `symkevi -> {CHEMBL2010601, CHEMBL3544914}`; `symkevi` is not
otherwise present as a label.

## Design decisions

1. **Replace, not additive** — drop the full `X component of Y` phrase from the
   emitted labels and emit only the extracted product. The phrase never matches
   a real query.
2. **Both source fields** — extract from `tradeNames` and `synonyms`.
3. **Tagging** — new `entitySource = "trade_name_component"`, `entityScore = 0.999`
   (same rank as exact trade names; distinct source keeps the contribution
   traceable in the LUT). No schema change: `entitySource` is a free-form string.
4. **Light cleanup** — strip leading/trailing `[\s/,;:-]+`, collapse internal
   whitespace, `trim`. Residual qualifier tails (e.g. `, glacial`) are rare and
   accepted.

## Implementation

### `common/utils.py`

```python
COMPONENT_OF_PATTERN = r'(?i) component of '

def extract_combination_product(label: Column) -> Column:
    """Extract and clean the combination product from a
    '{molecule} component of {product}' trade-name string.
    Returns '' when the label does not match the pattern."""
    product = f.regexp_extract(label, r'(?i)^.+ component of (.+)$', 1)
    product = f.regexp_replace(product, r'^[\s/,;:-]+', '')
    product = f.regexp_replace(product, r'[\s/,;:-]+$', '')
    product = f.regexp_replace(product, r'\s+', ' ')
    return f.trim(product)
```

`regexp_extract` returns `""` on no match; downstream the existing
`length > 0` filter discards those.

### `OpenTargetsDrug.as_label_lut`

- Build a `combinationProducts` array column = `extract_combination_product`
  applied over `concat(coalesce(tradeNames, []), coalesce(synonyms, []))`,
  keeping non-empty results, `array_distinct`.
- Filter the `component of` entries out of `tradeNames` and `synonyms`
  (`~x.rlike(COMPONENT_OF_PATTERN)`), so `Kalydeco` survives but the noise
  phrases do not.
- Annotate `combinationProducts` with
  `annotate_entity(col, "tbd", 0.999, "trade_name_component")` and fold it into
  the existing `flatten / array / explode`.

Everything downstream (normalisation, highest-score dedup, id-array collapse)
is unchanged: these are simply additional `RawEntityLUT` rows.

## Testing

New `tests/test_datasource_drug.py` (first datasource test; uses the `spark`
fixture from `conftest.py`). Builds a small in-memory drug index and asserts:

- `symkevi` is present, maps to the molecule id, `entitySource = "trade_name_component"`.
- The full `"ivacaftor component of symkevi"` phrase is **absent** from the labels.
- `Kalydeco` is still present with `entitySource = "trade_name"`.
- A noisy leading-`/` case has its punctuation stripped.

## Out of scope

- Disease/target datasources and the full OnToma LUT — the local output
  generation for impact reporting covers the **drug LUT only**.
- Aggressive normalisation of residual qualifier tails.

## Delivery workflow

1. Commit this spec on `feat/combination-trade-names`.
2. Open a GitHub issue in `opentargets/issues` (enhancement + impact table) —
   before implementation.
3. TDD implementation; `uv run pytest` green.
4. Generate the drug ready-LUT locally from the downloaded `drug_molecule`;
   report real before/after impact (record counts, `trade_name_component` rows
   surviving to the ready LUT).
5. Open a PR referencing the issue.
6. Codex review of the branch.
