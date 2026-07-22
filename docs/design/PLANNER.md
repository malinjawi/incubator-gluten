# Planner side: Scala changes for the fused rollup operator

Companion to `DESIGN.md`. Short by design — the JVM side is the easy half.

## 1. What changes in the existing rule

`LazyAggregateExpandRule` (branch commit `0f93d8c88`, not yet upstream) already does the hard part: it recognises
`Expand -> PartialAgg` over a rollup-shaped projection list and rewrites it to
`PartialAgg(finest, flushable) -> Expand(over buffers) -> PartialMerge(flushable) -> shuffle`.

Its matcher is exactly the matcher the fused path needs; the only change is what it re-emits.

The rule gains a second, narrower outcome:

* **Matched, and fusible** (see the table in §3): emit
  `PartialAgg(finest, flushable) -> Expand(marked isRollup)`, and drop the `PartialMerge` entirely.
  The `Expand` is preserved *as a carrier*, not as an executed operator — the native side reads its
  duplicate projections to recover the per-level grouping-id literals and null masks, then replaces
  it with a `RollupNode`. That is deliberate: it means the Substrait plan is unchanged in shape, the
  gid values remain Spark-computed, and a native library that does not know the marker executes a
  correct plan.
* **Matched, but not fusible**: emit today's three-operator rewrite, unchanged.

Two mechanical additions:

1. **Key ordering / projection.** The fused operator requires its input to be
   `ROW(k1..kn, acc1..accm)` with the keys in hierarchy order, coarsest first. The finest-grain
   `PartialAgg` the rule already inserts groups by all n keys; the rule must additionally pin the
   grouping-key order to the rollup order rather than whatever order the child produced. This is a
   sort of the grouping expression list, plus the corresponding output attribute reordering.
2. **The marker.** Set `isRollup=1` in the `optimization` field of the `AdvancedExtension` on the
   emitted `ExpandRel`, alongside the `allowFlush=` the rule already sets on the child aggregate.

   *Correction to an earlier revision:* this section previously claimed "the transform layer that
   builds `ExpandRel` needs to learn to carry an `AdvancedExtension`; today only the aggregate
   transformer does." That is wrong — `ExpandRelNode` already has an `AdvancedExtensionNode`
   constructor (`ExpandRelNode.java:36-41`), `RelBuilder.makeExpandRel` already has the matching
   overload (`RelBuilder.java:222-230`), and `ExpandExecTransformer` already calls it on the
   validation path (`ExpandExecTransformer.scala:88-95`). No plumbing work is needed.

   Two details that *do* matter. The validation path uses the `enhancement` field of the same
   extension, so the rule must build the node with
   `ExtensionBuilder.makeAdvancedExtension(optimization, enhancement)` rather than overwriting it.
   And the value must be exactly `1`: `configSetInOptimization` checks the single character after
   the `=` (`SubstraitParser.cc:285`), so `isRollup=true` would silently read as *false* — a bug
   that would show up as "the fused path never triggers and nobody notices".

## 2. Config

```
spark.gluten.sql.columnar.backend.velox.fusedRollup.enabled   (default: false)
```

Gated behind the existing `lazyAggregateExpand.enabled` — fused rollup is a refinement of that
rewrite, not an independent one. Turning the parent off turns this off. Flip the default to `true`
only after the rollout step 3 in `DESIGN.md` §9.

Add a documented entry to `velox-configuration.md` and regenerate, same as was done for
`lazyAggregateExpand.enabled`.

## 3. Fallback decision table

Evaluated in the rule, in order. The first `no` wins and the rule falls back to the three-operator
rewrite (never to the un-rewritten `Expand` plan — the landed rule remains the general path).

| Check | Fuse? | Why |
|---|---|---|
| `fusedRollup.enabled` | no if off | kill switch |
| Grouping sets form a strict descending prefix chain (i.e. ROLLUP) | no otherwise | CUBE and explicit GROUPING SETS need a lattice parent choice; v1 is a chain |
| `n >= 2` rollup keys | no otherwise | with one key the operator is a bypass lane plus a global agg; not worth a custom node |
| No aggregate has `isDistinct` | no otherwise | distinct needs a separate distinct-tracking structure per level |
| No aggregate has a `FILTER` mask | no otherwise | masks are a raw-input concept; the fused operator only sees states |
| No order-sensitive / holistic aggregate (`sortingKeys` non-empty) | no otherwise | merging states out of order is not defined for these |
| Every aggregate's Velox function has a usable merge companion | no otherwise | the fused operator only ever calls `addIntermediateResults` |
| Child partial aggregate is flushable (`allowFlush=`) | no otherwise | the whole design assumes partial-state output semantics |

A negative decision must be *cheap and silent* — this runs on every rollup query, and the fallback
is a perfectly good plan.

Testing: each row of this table gets a plan test asserting the emitted Substrait does **not** carry
`isRollup=`. The existing 21-case lazy-expand suite (`LazyAggregateExpandSuite.scala`) runs unchanged with the flag both off and on;
with the flag off it must produce byte-identical plans to today.

## 4. The ae-spark angle

The same operator concept ports to a Spark whole-stage-codegen operator without the Velox
dependency, and most of the design survives the move. What transfers is everything that is really
about *plan shape and state flow*: the finest-set bypass (rows out of the child's hash aggregate are
already unique, so tag and forward instead of re-hashing), the smallest-parent chain (derive level
`i-1` from level `i`'s output, turning `R x (n+1)` probes into `R + sum(G_i)`), the cascade-flush
state machine, and — crucially — the observation that a pre-shuffle operator emitting partial state
may *flush* duplicate groups rather than spill, which is what makes the whole thing bounded-memory
without a spill path. The per-level abandon valve transfers too, and so does the argument that an
abandoned merge level is a free pass-through.

What differs is everything about *representation*. Velox gives us `GroupingSet` with
`isPartial/isRawInput` bools that express "states in, states out" directly, and accumulators that
live in a `RowContainer` and never need materialising until a flush; a JVM implementation would sit
on `UnsafeRow`-backed aggregation buffers and `TungstenAggregationIterator`-style hash maps, so the
n+1 live per-level maps are n+1 real `UnsafeFixedWidthAggregationMap`s with their own page memory,
managed by the `TaskMemoryManager` rather than a Velox `MemoryPool`. Codegen changes the shape of the
work too: the cascade is a loop over levels in generated code rather than a driver-pulled state
machine, so backpressure and the `getOutput`-returns-one-batch contract disappear and are replaced by
the iterator model. And the arbitration question (`DESIGN.md` §4, memory arbitration) has a cleaner answer
on the JVM: `MemoryConsumer.spill()` is allowed to do arbitrary work, so "spill" can simply mean
"cascade a flush downstream" with no contract ambiguity. Net: the algorithm is portable, roughly
half the code is not.
