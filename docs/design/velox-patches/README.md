# Velox patches required by the fused grouping-set (rollup) operator

Gluten builds against an unmodified Velox **except** for the single patch below.

## 0001-hashagg-needsinput-fix.patch (REQUIRED, keep)

Adds `VELOX_CHECK_NULL(input_)` at the top of `HashAggregation::addInput` in
`velox/exec/HashAggregation.cpp` (plus the corresponding declaration note in
`HashAggregation.h`). Without it, when the driver feeds a second input batch
before draining the previous one via `getOutput()` — which happens under memory
pressure / partial-aggregation flush — the operator silently overwrites `input_`
and drops rows. This is a core-Velox correctness fix, so it cannot live inside
Gluten's own tree and must be carried as a Velox patch. The fused rollup operator
depends on this fix at runtime under memory pressure.

This patch is a standalone upstream-Velox candidate independent of the rollup work.

## 0002-fused-grouping-set-operator.patch (RETIRED — do not reapply)

**Superseded.** The fused grouping-set operator (`GroupingSetLattice`,
`GroupingSetAggregationNode`, `MultiGroupingSetAggregation`) no longer travels as
a Velox fork patch. It now lives **inside the Gluten tree** as a Gluten-local
custom Velox operator, mirroring the existing cudf custom-operator precedent:

- `cpp/velox/operators/rollup/GroupingSetLattice.h`
- `cpp/velox/operators/rollup/GroupingSetAggregationNode.h`
- `cpp/velox/operators/rollup/MultiGroupingSetAggregation.h`
- `cpp/velox/operators/rollup/MultiGroupingSetAggregation.cc`

It is compiled into `libvelox` (Gluten) via `cpp/velox/CMakeLists.txt` and
registered Gluten-locally by `registerMultiGroupingSetAggregation()`, called from
`cpp/velox/compute/VeloxBackend.cc`. The node keeps namespace
`facebook::velox::exec` so the Substrait converter
(`cpp/velox/substrait/SubstraitToVeloxPlan.cc`) and the registration call site
remain valid; only the `#include` paths changed to the new in-tree location.

Because the operator is Gluten-local, **no Velox fork is needed for it** — only
patch 0001 remains on Velox.
