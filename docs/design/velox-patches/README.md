# Velox patches required by the fused grouping-set (rollup) operator

The operator itself is not supplied by Velox. Gluten builds it from
`cpp/velox/operators/plannodes/`; the pinned Velox tree needs only the single
standalone patch below.

## `ep/build-velox/src/modify_hash_aggregation_input_buffer.patch` (REQUIRED, keep)

Adds `VELOX_CHECK_NULL(input_)` at the top of `HashAggregation::addInput` in
`velox/exec/HashAggregation.cpp` (plus the corresponding declaration note in
`HashAggregation.h`). Without it, when the driver feeds a second input batch
before draining the previous one via `getOutput()` — which happens under memory
pressure / partial-aggregation flush — the operator silently overwrites `input_`
and drops rows. This is a core-Velox correctness fix, so it cannot live inside
Gluten's own tree and must be carried as a Velox patch. The fused rollup operator
depends on this fix at runtime under memory pressure.

This patch is a standalone upstream-Velox candidate independent of the rollup work.
`ep/build-velox/src/build-velox.sh` applies it idempotently and refuses to
compile when neither the fix nor an applicable patch is present. This makes the
normal `buildbundle-veloxbe.sh` path enforce the same correctness prerequisite
as the isolated rollup test helper.

## 0002-fused-grouping-set-operator.patch (RETIRED — do not reapply)

**Superseded.** The fused grouping-set operator (`GroupingSetLattice`,
`GroupingSetAggregationNode`, `MultiGroupingSetAggregation`) no longer travels as
a Velox fork patch. It now lives **inside the Gluten tree** as a Gluten-local
custom Velox operator, mirroring the existing cudf custom-operator precedent:

- `cpp/velox/operators/plannodes/GroupingSetLattice.h`
- `cpp/velox/operators/plannodes/GroupingSetLattice.cc`
- `cpp/velox/operators/plannodes/GroupingSetAggregationNode.h`
- `cpp/velox/operators/plannodes/GroupingSetAggregationNode.cc`
- `cpp/velox/operators/plannodes/MultiGroupingSetAggregation.h`
- `cpp/velox/operators/plannodes/MultiGroupingSetAggregation.cc`

All three `.cc` files are compiled into `libvelox` (Gluten) via
`cpp/velox/CMakeLists.txt`, and the operator is
registered Gluten-locally by `registerMultiGroupingSetAggregation()`, called from
`cpp/velox/compute/VeloxBackend.cc`. The node keeps namespace
`facebook::velox::exec` so the Substrait converter
(`cpp/velox/substrait/SubstraitToVeloxPlan.cc`) and the registration call site
remain valid; only the `#include` paths changed to the new in-tree location.

Because the operator is Gluten-local, **no Velox fork is needed for it** — only
`ep/build-velox/src/modify_hash_aggregation_input_buffer.patch` remains on Velox.

## Archived prototype sources (reference only)

`docs/design/RollupNode.h`, `RollupOperator.{h,cpp}`,
`RollupTranslator.cpp`, and `RollupOperatorTest.cpp` are historical
proof-of-concept artifacts. They are deliberately outside the build, do not
define the production symbols, and must not be applied to Velox. Their
production successors are the Gluten-local files listed above and
`cpp/velox/tests/MultiGroupingSetAggregationTest.cc`.
