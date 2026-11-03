Title: [GLUTEN-12263][VL] Write Lance fixed-size list columns natively

<!-- Draft, not posted. Open only after the core writer PR merges and only with the user's OK.
     Squash fixup 7372e9fdd into the vectors commit first (see handoff/README.md). -->

## What changes are proposed in this pull request?

Follows #<core writer PR>. Velox has no fixed-size list type and exports Lance's vector columns as plain lists, so the native Lance write falls back for any table with a vector column.

The writer now imports such batches as Velox exports them and moves each top-level fixed-size list column of signed integers or floats into the commit schema's fixed-size list vector:
- without a copy when every row holds a list
- with one memory copy per row when some rows are null; a null row's element slots are zeroed
- element by element only when the elements themselves hold nulls

A list with the wrong number of elements fails the task.

Fixed-size lists nested in other columns, and half-float vectors, still fall back to lance-spark's writer.

## How was this patch tested?

`VeloxLanceWriteSuite` gains 2 tests: vector columns written natively (null vectors, small batches, a filter and an overwrite), and a list of the wrong size failing the write. `LanceFixedSizeListsSuite` (5 tests) unit-tests the conversion, including null elements and a non-zero first list offset. Spark 3.5 and 4.0.

Gluten's Lance scan cannot read fixed-size lists yet (#13487, fixed by #13488), so the tests read vectors back with lance-spark's reader.

Local benchmark (laptop, `local[12]`, Spark 3.5, 1M rows with a 768-float vector, median of 3): 0.95 s with the native writer, against 1.59 s for vanilla Spark (1.7x) and 1.61 s for Gluten with lance-spark's writer. With 1% null vectors: 1.11 s against 1.38 s (1.2x). All setups wrote the same data.

## Was this patch authored or co-authored using generative AI tooling?

Generated-by: IBM Bob
