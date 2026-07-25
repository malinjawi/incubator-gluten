# Fused Rollup Operator for the Gluten Velox backend (historical Phase 3 draft)

Status: **historical prototype design, retained for audit context**. The
`docs/design/Rollup*` source files discussed here are non-building proof-of-concept
artifacts, not the implementation. The production operator is Gluten-local under
`cpp/velox/operators/plannodes/`:
`GroupingSetAggregationNode.{h,cc}`, `GroupingSetLattice.{h,cc}`, and
`MultiGroupingSetAggregation.{h,cc}`. Current readiness and deployment caveats live in
`DEFENSIBILITY.md` and `DEPLOYMENT_RUNBOOK.md`; production source wins over this draft
where the design evolved.

Author's note: items marked **STILL OPEN** could not be settled from a direct repo read. Items
marked **RESOLVED** were open in an earlier revision and have since been checked against the real
Velox and Gluten sources available when this prototype was written. §11 lists both, plus the
defects found and fixed during that review.

---

## 1. Problem

Spark lowers `GROUP BY ROLLUP(k1..kn)` into `Expand` (n+1 projections) followed by a partial
aggregation. Every input row is materialised n+1 times before any reduction happens, so the partial
aggregation hash-probes `R x (n+1)` rows. For TPC-DS q67 (8 rollup keys, 9 grouping sets) that is a
9x row amplification in front of the most expensive operator in the stage.

Two prior data points:

* **#12052** (non-flushable lazy expand) measured only ~10% end-to-end on q67. Without a flushable
  partial aggregate the finest-grain aggregation had to be blocking, which moved the cost rather
  than removing it.
* **The `LazyAggregateExpandRule` on this branch** (commit `0f93d8c88`,
  `backends-velox/src/main/scala/org/apache/gluten/extension/LazyAggregateExpandRule.scala`, 355
  lines, 21-case suite) rewrites the plan to
  `flushable partial agg (finest grain) -> Expand over aggregation buffers -> flushable PartialMerge -> shuffle`.

  **Correction to an earlier revision of this document:** that rule was described here as "#12554
  (landed JVM rule)" with "~3.1x at stage level on a q67-shaped synthetic". Neither holds up. It is
  an unmerged commit on the local feature branch `claude/gluten-velox-aggregate-opt-e1be35`, not a
  landed upstream PR, and there is no PR number 12554 referenced anywhere in the tree. The 3.1x
  figure has no benchmark artifact in the repo and should be treated as unsubstantiated until it is
  reproduced. Everything downstream in this document that reasons "on top of the 3.1x the rule
  already delivered" inherits that uncertainty.

The prior rule is a large win *if the number holds*, but it still pays the amplification once: the `Expand` now
duplicates *aggregation buffers* instead of raw rows, and the downstream `PartialMerge` still
hash-probes `S x (n+1)` rows, where `S` is the number of finest-grain states.

This document proposes collapsing those three operators into one Velox operator that never
materialises the amplified stream at all.

Prior art worth naming: Databricks Photon ships a fused `PhotonGroupingAggWithRollup` operator
(visible in a real q67 plan), and Gluten's own ClickHouse backend already has a two-port fused
lazy-expand operator at `cpp-ch/local-engine/Operator/AdvancedExpandStep.cpp`. The hierarchical
derivation fallback is the classic smallest-parent rule from Agarwal et al., *On the Computation of
Multidimensional Aggregates* (VLDB 1996). The native lattice API also accepts optional per-set row
estimates and prefers the strict-superset parent expected to emit fewer rows; the current Gluten
planner does not yet transport reliable subset NDV estimates, so production uses the deterministic
smallest-parent fallback.

---

## 2. Plan shapes

Let the rollup keys be `k1..kn` (ordered coarse to fine, so level `i` groups by the prefix
`k1..ki`), and let `a1..am` be the aggregates.

### 2.1 Today (no rule)

```
Shuffle
  PartialAgg  [group by k1..kn, gid]      <- probes R x (n+1)
    Expand    [n+1 projections]           <- emits R x (n+1) raw rows
      Scan/Project                        <- R rows
```

### 2.2 With the existing LazyAggregateExpandRule (branch commit `0f93d8c88`)

```
Shuffle
  PartialMerge [group by k1..kn, gid]     <- probes S x (n+1)
    Expand     [n+1 projections over buffers]
      PartialAgg [group by k1..kn, flushable]  <- probes R, emits S states
        Scan/Project
```

The first aggregation now absorbs the cardinality reduction, so `S << R` and the amplified stream
is much cheaper. But it is still built, still shuffled through a hash table, and each amplified row
carries `m` accumulator columns that have to be extracted to a row-friendly encoding and
re-consumed.

### 2.3 Proposed fused shape

```
Shuffle
  Rollup       [hierarchy k1..kn, per-level gid, cascade flush]   <- probes ~ sum(G_i)
    PartialAgg [group by k1..kn, flushable]
      Scan/Project
```

The `Rollup` operator consumes finest-grain **intermediate accumulators** and emits, on one output
port, the union of all n+1 levels — also as intermediate accumulators, with the Spark-supplied
`gid` and null-masked key columns. The post-shuffle final aggregation is unchanged.

Three things fusion buys that the three-operator shape cannot:

1. **Finest-set bypass.** The rows arriving from the child partial aggregate are already grouped at
   the finest grain. Level `n` therefore needs *no hash table at all*: tag each input row with
   `gid_n` and forward it. This is the same trick as the ClickHouse backend's two-port
   `AdvancedExpandStep`. On q67 this removes the single largest table from the memory budget.
2. **No extract/reconstruct round trip.** The operator holds Velox-native accumulators end to end.
   The three-operator shape has to `extractAccumulators` into vectors, push them through `Expand`'s
   projections, and re-consume them via `addIntermediateResults`. Fusion keeps the states inside
   `RowContainer`s and only materialises them when a level flushes.
3. **Smallest-parent hierarchical merging.** Level `i-1` is derived from level `i`'s *output states*,
   not from the operator input. Total merge work becomes `R + sum_{i=1..n} G_i` instead of
   `R x (n+1)`, where `G_i` is level `i`'s distinct-group count.

---

## 3. Operator algorithm

### 3.1 Level layout and row shapes

Levels are indexed by the number of live keys, `L = n .. 0`:

* `L = n` — the **bypass lane**. No hash table.
* `L = n-1 .. 1` — a `GroupingSet` keyed on `k1..kL`.
* `L = 0` — the grand total; a `GroupingSet` with an empty hasher vector, which Velox treats as a
  global aggregation (`GroupingSet.cpp:62`) and merges through
  `Aggregate::addSingleGroupIntermediateResults` (`GroupingSet.cpp:660-661`).

Define the **natural row type** of level `L`:

```
naturalType(L) = ROW(k1 .. kL, acc1 .. accm)
```

This is exactly the shape `GroupingSet::extractGroups` produces for a level with `L` keys and
`isPartial_ = true` (keys first, then one intermediate-typed column per aggregate —
`GroupingSet.cpp:847-888`, `:871-876`).

The table for level `L` is constructed over input type `naturalType(L+1)`, with grouping key
channels `0..L-1` and aggregate input channels `L+1 .. L+m`. So the operator's own input type is
required to be `naturalType(n)` — keys first, in hierarchy order, then the intermediate accumulator
columns. **The JVM side must project the child partial aggregate's output into that order**; this is
a hard precondition, checked with a `VELOX_CHECK` in `initialize()`.

The single output row type is uniform across levels:

```
outputType = ROW(k1 .. kn (all nullable), acc1 .. accm, gid BIGINT)
```

A level-`L` row is projected into it by passing `k1..kL` through, substituting constant NULL for
`k(L+1)..kn`, and appending the constant `gid_L`. Both the gid literals and the per-level null mask
come from the JVM (they are already present as literals in the `ExpandRel` projections) — we never
recompute Spark's bitmask in C++.

### 3.2 Steady state

**Indexing note.** This document indexes levels by `L`, the number of *live keys* (`L = n` is the
bypass lane, `L = 0` the grand total). The code indexes them finest-first by array position `i`,
with `i = n - L`, so `levels_[0]` is the bypass and `levels_[n]` the grand total. Below, `level n-1`
means "one key coarser than the bypass", i.e. `levels_[1]` in code.

`addInput(input)`:

1. Project `input` into the output shape with `gid_n` and push it onto the bypass output queue.
   (Zero hashing, zero copying of the accumulator columns — they are shared by reference.)
2. `levels_[n-1].groupingSet->addInput(input, /*mayPushdown=*/false)`.
3. Check `isPartialFull(maxPartialAggregationMemoryUsage_)` on each live level; if any level is
   full, record the *finest* full level as `drainLevel_`, switch to `kPressureDrain`, and stop
   accepting input until it has been drained.

`needsInput()` is false while the bypass queue is non-empty or a drain is in progress.

Because `needsInput()` is false during a pressure drain, `noMoreInput()` can and does arrive
mid-drain. It must not restart the cascade at the finest level while a `RowContainerIterator` is
half-consumed; the in-flight drain finishes first and the final cascade is deferred
(`pendingFinalCascade_`).

`getOutput()`:

1. If the bypass queue is non-empty, pop and return one vector.
2. Otherwise, if a drain is in progress, pull one batch from the draining level (see 3.3).
3. Otherwise return `nullptr`.

### 3.3 Cascade flush

This is the core of the design and the part I most want reviewed.

Because the operator sits **before the shuffle** and emits **partial** states, duplicate group
states in the output are legal — the post-shuffle final aggregation merges them. That is the same
contract Velox's own partial `HashAggregation` relies on, and it is why these tables **flush**
rather than **spill**.

When level `L` drains (either under memory pressure or at end of input), each batch it produces is
used **twice**:

* projected into the output shape with `gid_L` and returned from `getOutput()`, and
* fed into level `L-1`'s `GroupingSet::addInput` — because `naturalType(L)` is precisely level
  `L-1`'s declared input type.

Each state therefore flows down the hierarchy exactly once. After level `L` is fully drained we call
`resetTable(/*freeTable=*/false)` (mirroring `HashAggregation::resetPartialOutputIfNeed`,
`HashAggregation.cpp:286-311`) and, if we are in the end-of-input cascade, move on to level `L-1`.

`noMoreInput()` sets the state to `kFinalCascade` with the drain cursor at level `n-1`; the cascade
walks finest to grand total and the operator is finished when level 0 has been drained and every
queue is empty.

**Memory caveat (current correction to this historical draft):** the operator can own one live
table for every non-bypassed, non-abandoned grouping set. Its retained aggregate state is therefore
`O(sum_i(G_i * W_i))`, where `G_i` is the live group count and `W_i` is that level's state width;
it is not bounded to two tables. Draining level `L` into level `L-1` can also make extracted output
vectors and parent growth coexist temporarily. Proactive soft/shared-target flushes, descendant
hard-cap checks, and per-level abandonment control this state, but input batches, hash-table
capacity jumps, and extraction buffers mean they are scheduling limits rather than a strict
allocator cap. There is no live-state spill or reclaim path.

### 3.4 Per-level abandon valve

Velox's partial aggregation gives up when reduction is poor:
`numInputRows_ > abandon_partial_aggregation_min_rows` and
`100 * numOutput / numInput >= abandon_partial_aggregation_min_pct`
(`HashAggregation.cpp:183-187`). We apply the same test **per level, except the grand total** —
`abandonPartialAggregationEarly` `VELOX_CHECK`s `isPartialOutput_ && !isGlobal_`
(`HashAggregation.cpp:184`), and the caller guards on `!isGlobal_` too (`:207-211`). A global level
holds exactly one group, so there is nothing to give up on.

An abandoned level is trivially cheap here, and this falls out of the semantics rather than needing
machinery: since a merge level has `isRawInput = false`, "abandon" means *forward the incoming
states, re-projected with this level's null mask and gid*. No accumulator work at all — but note
"unchanged" is too strong: the batch must still be reshaped from the level below's row type
(`ROW(k1..k[L+1], accs)`) to this level's (`ROW(k1..kL, accs)`) before it is forwarded, or the next
level receives a vector whose type is not the type its `GroupingSet` was constructed over. That is
column reshuffling, not data movement.
(`GroupingSet::toIntermediate` already short-circuits to a pass-through when `!isRawInput_`,
`GroupingSet.cpp:1598-1601`, which confirms the semantics — but we do not need to route through
`GroupingSet::abandonPartialAggregation()` for it; a direct projection is simpler and exact.)

Abandoning level `L` pushes its input volume down to level `L-1`, which is the same behaviour the
three-operator plan has by construction, so the fused operator degrades gracefully to "no worse than
today" rather than falling off a cliff.

Each non-bypassed grouping set keeps the ordinary
`max_partial_aggregation_memory` soft threshold, so low-cardinality coarse sets
are not starved merely because several tables exist. Post-flush growth still
doubles a node's threshold up to
`max_extended_partial_aggregation_memory`.
Explicit pool reservation is operator-coordinated: it reuses already-unused
reservation and requests only headroom useful before the shared target. This
avoids paying Velox's 8 MiB reservation quantum once per small node-local
growth.

The extended limit is also the target for the sum of all flushable
grouping-set allocations, including global aggregate state. This is a
scheduling target, not a strict allocator cap: a whole input batch, hash-table
capacity jump, or parent drain batch can temporarily overshoot it. At every safe
boundary the operator drains local hard/soft violations first, then the largest
eligible set, until another input batch can be accepted without actionable
over-target state. A global pressure drain emits its mergeable intermediate row
and calls `resetGlobalAggregation()` before accepting more input.

### 3.5 Worked example (q67-shaped)

q67 rolls up 8 keys (`i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id`),
so 9 levels. Per-task figures below are illustrative of an SF-scale shape, not measured:

| Level `L` | keys live | `G_L` (distinct groups) | table? |
|---|---|---|---|
| 8 | all | 2,000,000 (`= R`, the input states) | **no — bypass** |
| 7 | 7 | 900,000 | yes |
| 6 | 6 | 300,000 | yes |
| 5 | 5 | 120,000 | yes |
| 4 | 4 | 40,000 | yes |
| 3 | 3 | 8,000 | yes |
| 2 | 2 | 600 | yes |
| 1 | 1 | 30 | yes |
| 0 | 0 | 1 | yes (global) |
| | | **total output ≈ 3,368,631** | |

Probe accounting:

* **Landed rule (2.2):** `Expand` emits `S x 9 = 18,000,000` rows, all of which are hash-probed by
  the `PartialMerge`. Plus 18M accumulator extract/reconstruct round trips.
* **Fused (2.3):** bypass costs 0 probes; level `L` probes `G_{L+1}` rows. Total probes
  `= 900k + 300k + 120k + 40k + 8k + 600 + 30 + 1 + 2,000,000(into level 7) ≈ 3.37M`.

That is ~5.3x fewer probe operations than the three-operator rule (whose own speedup is
unsubstantiated — see §1). I would *not* claim 5.3x end-to-end: probes at coarse levels hash fewer, narrower keys
and are individually cheaper; the shuffle volume is identical in both shapes (both emit ~3.37M
rows); and Amdahl applies once the stage is no longer aggregation-bound. A defensible prediction is
**1.3–1.8x additional stage-level speedup**, dominated by (i) never building the 18M-row stream and
(ii) never round-tripping accumulators. Getting a real number is the first milestone, not the last.

Memory: the bypass removes the 2M-group table entirely, so peak footprint is driven by level 7's
900k groups. In a naive "one table per level" fused design it would have been 2.9M groups.

---

## 4. Memory and correctness model

### 4.0 Why cascading from flushed (duplicated) partial states is still correct

This is the load-bearing correctness argument, so it is worth stating properly rather than asserting.

Let `⊕` be an aggregate's state-merge operator (what `Aggregate::addIntermediateResults` applies).
For every aggregate admitted in v1, `⊕` is associative and commutative — that is exactly the
contract that makes Velox's own `kPartial -> kFinal` split legal, and v1 excludes the aggregates for
which it does not hold (order-sensitive, DISTINCT, masked; §6).

Let level `L`'s output over the whole task be the multiset
`O_L = { (p, s) }` of (key-prefix, state) pairs, emitted across however many flushes occurred.
Define `agg(O_L, p) = ⊕{ s : (p, s) ∈ O_L }`.

**Claim.** If level `L` is correct — `agg(O_L, p)` equals the true level-`L` aggregate for every
prefix `p` — and level `L-1` is fed exactly `O_L` and groups it by the shorter prefix `p'`, then
level `L-1` is correct.

**Proof.** `agg(O_{L-1}, p') = ⊕{ s : (p, s) ∈ O_L, p|_{L-1} = p' }`. By associativity and
commutativity this equals `⊕{ agg(O_L, p) : p|_{L-1} = p' }`, which by the hypothesis is the merge
of the true level-`L` aggregates over all `p` refining `p'` — i.e. the true level-`L-1` aggregate. ∎

Two things fall out, and both are the answers to the obvious attacks:

* **Flushing does not break it.** The proof never assumes `O_L` has one entry per prefix. Duplicate
  `p` entries are merged by the same `⊕`; how the states were partitioned across flushes is
  invisible to the result. This is why levels may flush freely.
* **The bypass lane does not break it either.** Level `n`'s "output" is literally the operator's
  input. If the upstream partial aggregate flushed or abandoned, that input contains duplicate
  finest-grain keys. Correctness is unaffected for the same reason: `O_n` with duplicates still
  satisfies `agg(O_n, p) =` true level-`n` aggregate, so every level below inherits correctness.
  What *is* lost is reduction — an abandoned upstream means `|O_n| = R` and level `n-1` sees the full
  raw volume. That is the un-fused cost, i.e. graceful degradation, not a correctness cliff.
  Note also that an abandoned upstream `HashAggregation` still emits **intermediate states**, not
  raw rows (`GroupingSet::toIntermediate`, `GroupingSet.cpp:1593-1601`), so the bypass stays
  type-correct in that case.

The one requirement the proof *does* impose is that **each state flows into the next coarser level
exactly once** — not zero times, not twice. That is a property of the state machine (§3.3), not of
the algebra, and it is what the forced-flush and mid-cascade-`noMoreInput` tests in §8 exist to
protect.

* Output is **partial state**, pre-shuffle. Duplicate group keys across flushes are correct.
* Therefore: **flush, do not spill.** Every level constructs its `GroupingSet` with
  `spillConfig = nullptr`, which is the precedent `GroupingSet::createForDistinct` already sets
  (`GroupingSet.h:62-68`).
* Determinism: output row order is not stable across flush timing. Aggregates must be
  merge-associative, which every non-holistic Velox aggregate is. Order-sensitive aggregates are
  excluded in v1 (§6).

**RESOLVED (was OPEN QUESTION 2) — memory arbitration.** With `spillConfig = nullptr`,
`Operator::canSpill()` is false and therefore `canReclaim()` (`Operator.h:398-400`) is false, so
under global memory pressure the arbitrator cannot ask this operator for anything and may kill the
query instead. Overriding `canReclaim()` to return true and implementing `reclaim(targetBytes,
stats)` (`Operator.h:419-421`) is the fix, and reading `Operator::MemoryReclaimer::reclaim`
(`Operator.cpp:745-804`) settles both halves of the original question:

* **May `reclaim()` produce output?** Yes. The call is made with the driver off-thread, suspended or
  terminated, and the task paused — `VELOX_CHECK`s at `Operator.cpp:758-765`. Nothing forbids
  mutating operator state or building vectors; parking batches on `outputQueue_` is legal.
* **The reclaimer is installed regardless of spilling.** `maybeSetReclaimer()`
  (`Operator.cpp:113-121`, called from `Operator::initialize()` at `:216`) attaches
  `Operator::MemoryReclaimer` whenever the parent pool has one — it does not consult `spillConfig`.
  So overriding `canReclaim()` genuinely puts this operator in the arbitrator's reach.

**The real constraint turns out to be different, and sharper.** The `op_->reclaim()` call is wrapped
in `memory::ScopedReclaimedBytesRecorder` and immediately followed by

```
VELOX_CHECK_GE(reclaimedBytes, 0,
    "Unexpected memory growth after reclaim from operator memory pool {}");
```

(`Operator.cpp:788-795`). Reclaim must be **net non-allocating on this operator's pool**. Draining a
level allocates the extracted `RowVector` — and, for variable-width accumulators, copies out of the
string allocator — *before* `resetTable` frees the container. So "cascade a flush" is a legal
reclaim strategy only when the freed table bytes exceed the parked output bytes. Usually true (a
`RowContainer` plus hash table dwarfs one output batch), but not guaranteed for a poorly-reducing
level with variable-width state.

This remains the highest-risk item in the design, but it is now a **measurable engineering question**
("does a level drain net-free memory?") rather than an unknown contract. Mitigations if it does not
hold: drain in small batches and stop once `targetBytes` is met; or free the table first and
re-extract lazily; or fall back to `canReclaim() == false`.

---

## 5. Planner and Substrait integration

### 5.1 Where the node is created

`SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::Rel&)`
(`SubstraitToVeloxPlan.cc:1665-1699`) is the dispatch chain. There is no `RollupRel` in Substrait
and inventing one is not worth it, so the recommendation is:

**Keep emitting the existing `ExpandRel(AggregateRel(...))` shape and mark it.** Gluten's established
marker idiom is `SubstraitParser::configSetInOptimization(rel.advanced_extension(), "allowFlush=")`
(`SubstraitToVeloxPlan.cc:279-280`) / `"isStreaming="` (`:598-599`). We add `"isRollup="` and detect
it inside `toVeloxPlan(const ::substrait::ExpandRel&)` (`:899-938`): if the marker is present, build
a `RollupNode` instead of a `core::ExpandNode`, and merge the child `AggregateRel`'s aggregate list
into it.

This is preferable to the ClickHouse backend's pure shape-sniffing because the decision stays on the
JVM side where the cost model lives, and it is preferable to a new rel because it needs no protobuf
change and degrades safely: an older native library ignores the marker and executes the correct,
slower `Expand + PartialMerge` plan.

Crucially, **the per-level gid literals and null masks are already in the `ExpandRel`**: each
`fields()[i].switching_field().duplicates()` entry is either a selection or a literal
(`SubstraitToVeloxPlan.cc:924`). Parsing them out gives us Spark's exact gid values and exact null
placement without recomputing anything.

**RESOLVED (was OPEN QUESTION 3) — `ExpandRel` does carry an `advanced_extension`.** The proto has
`substrait.extensions.AdvancedExtension advanced_extension = 10;` on `message ExpandRel`, and the
JVM plumbing already exists: `ExpandRelNode` has an `AdvancedExtensionNode` constructor
(`ExpandRelNode.java:36-41`) and `RelBuilder.makeExpandRel` the matching overload
(`RelBuilder.java:222-230`). No fallback needed.

Two details the rule must respect:

* `ExpandExecTransformer` already uses that slot on the **validation** path to ship the input type
  struct (`ExpandExecTransformer.scala:88-95`). That writes `enhancement`; the marker idiom writes
  `optimization` (`SubstraitParser::configSetInOptimization` reads `extension.optimization()`,
  `SubstraitParser.cc:281-289`). No collision, but use
  `ExtensionBuilder.makeAdvancedExtension(optimization, enhancement)` rather than replacing the node.
* The marker convention is not "key present": `configSetInOptimization` requires the character
  immediately following the key to be `'1'` (`SubstraitParser.cc:285`). Emit `"isRollup=1"`.

**Correction to an earlier revision:** the level/column indexing of `ExpandRel` was stated
backwards. The substrait message *reads* column-major (`fields` → `SwitchingField` → one expression
per `duplicate_id`), but Gluten writes it **level-major**: `ExpandRelNode.java:60-69` emits one
`ExpandField` per projection *set*, whose `duplicates` are that set's *columns*, and
`SubstraitToVeloxPlan.cc:912-928` reads it back the same way. So the access is
`expandRel.fields(level).switching_field().duplicates(column)`, and `fields_size()` is `n + 1`.

### 5.2 Registration

Gluten already registers a custom Velox operator this way — `VeloxBackend.cc:183` calls
`velox::exec::Operator::registerOperator(std::make_unique<CudfVectorStreamOperatorTranslator>())`
under `#ifdef GLUTEN_ENABLE_GPU`. We add an unconditional
`Operator::registerOperator(std::make_unique<RollupOperatorTranslator>())` next to it.
`LocalPlanner.cpp:716` picks the node up automatically via `Operator::fromPlanNode` once the
built-in `dynamic_pointer_cast` chain falls through.

No `DriverAdapter` is needed (and Gluten uses none today).

Registering from `cpp/velox` first buys iteration speed; the operator has no Gluten-specific
dependencies, so upstreaming to `facebookincubator/velox` later is a file move plus serde.

**RESOLVED (was OPEN QUESTION 4) — header availability.** `cpp/velox/CMakeLists.txt:283-289` adds
`${VELOX_HOME}` itself to the include path — Gluten compiles against the whole Velox source tree,
not an installed subset — and it already includes `velox/exec/HashTable.h` at exactly this depth
(`cpp/velox/operators/hashjoin/HashTableBuilder.h:23`, `cpp/velox/jni/JniHashTable.h:25`). So
`GroupingSet.h`, `AggregateInfo.h`, `AggregateFunctionRegistry.h` and `RowContainer.h` are all
reachable. They are still unstable internal APIs with no compatibility guarantee, which remains an
argument for upstreaming early — but it is not a blocker.

**STILL OPEN (§11 item 3) — serialization.** `PlanNode::serialize()` has a base implementation, but a
`RollupNode` round trip needs a registered deserializer, and `PlanNode::registerSerDe()`
(`PlanNode.cpp:3982-4022`) only knows built-ins. Gluten's shipped custom node just throws
`VELOX_UNSUPPORTED` (`CudfVectorStream.h:153-155`). I follow that precedent in the draft, which means
plan tracing/replay tooling will not work on rollup stages. Acceptable for v1? Or do we register
serde from day one?

**STILL OPEN (§11 item 4) — barriers.** `AggregationNode::supportsBarrier()` returns true
(`PlanNode.h:1301`) and `Operator::startDrain()` is `VELOX_NYI` by default (`Operator.h:260-263`).
The draft leaves both at the default (no barrier support). If Gluten's Velox path relies on barriers
anywhere, this needs implementing — and it maps naturally onto the existing cascade.

---

## 6. v1 scope and fallback matrix

v1 handles **ROLLUP only** — a strict prefix hierarchy, which is what makes smallest-parent a chain
rather than a lattice. CUBE and arbitrary GROUPING SETS need a lattice ordering (pick a minimum-cost
parent per set); the operator's level machinery generalises, the planner side does not, so it is
deferred.

| Condition | v1 behaviour |
|---|---|
| ROLLUP, no DISTINCT/FILTER, all aggregates mergeable | **fused `RollupNode`** |
| CUBE or explicit GROUPING SETS | fall back to the existing 3-stage rewrite (`LazyAggregateExpandRule`) |
| Any aggregate with `DISTINCT` | fall back |
| Any aggregate with a `FILTER` mask | fall back |
| Holistic / order-sensitive aggregate (sorting keys present) | fall back |
| Rollup over a single key (n = 1) | fall back — one bypass lane + one global level; not worth it |
| Fused path disabled by config | fall back |
| Native library does not recognise the marker | fall back (marker ignored) |

The three-stage rewrite stays in the tree as the general path. The fused operator is strictly an
opt-in fast path for the common shape, which keeps the blast radius small.

---

## 7. Metrics

Emitted through `Operator::addRuntimeStat` (`Operator.h:359-362`), mirroring the naming Velox already
uses for partial aggregation (`kFlushRowCount`, `kFlushTimes`, `kPartialAggregationPct`):

Operator-wide:
* `rollup.numLevels`
* `rollup.bypassRows` — rows that skipped hashing entirely
* `rollup.cascadeFlushes` — pressure-triggered cascades (excludes the end-of-input cascade)
* `rollup.peakLevelBytes`

Per level `L`:
* `rollup.L<L>.inputRows`, `rollup.L<L>.outputRows`
* `rollup.L<L>.aggregationPct` — `100 * output / input`; the number that drives the abandon valve
* `rollup.L<L>.flushTimes`, `rollup.L<L>.flushRowCount`
* `rollup.L<L>.abandoned` — 0/1
* `rollup.L<L>.hashTableBytes`

The per-level `aggregationPct` series is the diagnostic that tells us, from a production query,
whether the hierarchy is worth fusing at all — if levels 7 and 6 both sit near 100%, the smallest-
parent chain is not reducing and the plan should have fallen back.

---

## 8. Test plan

**Velox-level (C++ operator tests):**

1. *Differential correctness.* For randomised inputs, run (a) the fused `RollupNode` and (b) the
   reference `ExpandNode + AggregationNode(kIntermediate)` plan, apply the same final aggregation to
   both outputs, and assert equality as multisets. This is the load-bearing test: it validates
   duplicate-state legality, gid values, and null masks in one shot.
2. *Forced flush.* Same as (1) with `max_partial_aggregation_memory` set to a few KB so every level
   flushes many times, including mid-cascade re-entry.
3. *Forced abandon.* Same as (1) with `abandon_partial_aggregation_min_rows = 0` and
   `min_pct = 0`, so every level abandons immediately and the operator degenerates to pure
   projection into the grand total.
4. *Boundary shapes.* n = 2; all input rows identical; all input rows distinct at every level; a
   single input batch; zero input batches (must still emit the grand-total row per Spark ROLLUP
   semantics — **verify this against the JVM side**, it is a classic off-by-one).
5. *Aggregate coverage.* At minimum one fixed-width (`sum`), one variable-width/external-memory
   (`avg` on decimal, `collect_list`), and one that reports `accumulatorUsesExternalMemory()`.

**Gluten e2e:** reuse the existing 21-case lazy-expand suite (`LazyAggregateExpandSuite.scala`) unchanged, run twice — once with the
fused flag off (must reproduce today's results exactly) and once on. Add TPC-DS q67, q27, q36, q70
(the rollup-shaped queries) with result comparison against vanilla Spark. Fallback matrix rows each
get a test asserting the plan *did not* use `RollupNode`.

---

## 9. Rollout

1. Operator + node + translator behind
   `spark.gluten.sql.columnar.backend.velox.fusedGroupingSetAggregate.enabled`,
   default **false**. Correctness tests green.
2. Benchmark: q67-shaped synthetic first (the LazyAggregateExpandRule baseline must be measured too, not assumed), then full TPC-DS SF1000.
   Publish per-level `aggregationPct` alongside the timing so the win is explainable.
3. Enable by default for ROLLUP-only shapes if the benchmark holds and no regression appears in the
   fallback matrix.
4. CUBE / GROUPING SETS via lattice ordering — separate PR.
5. Upstream the operator to `facebookincubator/velox` once the API surface has settled (this also
   the internal-header exposure is the standing argument for it).

---

## 10. Effort estimate

| Item | Estimate |
|---|---|
| `RollupNode` + translator + registration | 2 days |
| Operator: level construction, bypass, steady state | 4 days |
| Cascade flush state machine + abandon valve | 5 days (the risky part) |
| `reclaim()` integration (pending the net-non-allocating measurement, §4) | 2–5 days |
| Substrait hook + gid/null-mask extraction | 2 days |
| Scala rule generalisation + config + fallback matrix | 3 days |
| Velox unit tests (1–5 above) | 4 days |
| Gluten e2e + benchmark runs | 3 days |
| **Total** | **~5–6 weeks of focused work**, plus review latency |

The cascade state machine and the arbitration question are where the schedule risk lives. Everything
else is mechanical.

---

## 11. Consolidated open questions

Resolved by direct repo read during review — see the linked sections for the evidence:

| # | Question | Status |
|---|---|---|
| 2 | May `reclaim()` produce output? | **Resolved.** Yes; the real constraint is that reclaim must be net non-allocating (§4) |
| 3 | Does `ExpandRel` carry `advanced_extension`? | **Resolved.** Yes, field 10; JVM plumbing already exists (§5.1) |
| 4 | Are the internal Velox headers reachable from `cpp/velox`? | **Resolved.** Yes, whole source tree is on the include path (§5.2) |
| 7 | `toAggregateInfo`'s `expressionEvaluator` argument | **Moot.** `toAggregateInfo` is not usable here at all — it dereferences `sources()[0]` (`AggregateInfo.cpp:49`). `AggregateInfo` is now built by hand, following `ColumnStatsCollector::createAggregates` |
| 8 | `exec::SpillStats` argument type | **Resolved.** It is `exec::SpillStats*` (`SpillStats.h:27`) and `nullptr` is accepted (`ColumnStatsCollector.cpp:151`) |

Still genuinely open:

1. **Budget partitioning** across `n` live level tables (§3.4). Unchanged — still needs a decision
   and probably a new config key.
2. **Does a level drain net-free memory?** (§4) The `reclaim()` contract question is answered; this
   measurement replaces it as the highest-priority risk.
3. **Serde for `RollupNode`** — throw like `CudfValueStreamNode`, or register properly? (§5.2)
4. **Barrier / `startDrain()` support** — needed by Gluten's Velox path? (§5.2)
5. **Empty input** — must a ROLLUP still emit the grand-total row? (§8, test 4). Note the fused
   operator gets this for free in one direction: the grand-total level is a global `GroupingSet`,
   and `getGlobalAggregationOutput` (`GroupingSet.cpp:671-707`) emits its single row whether or not
   any input arrived. Whether that matches Spark's semantics for an empty ROLLUP still needs
   checking against the JVM side.

### Defects found in review and fixed in the draft

Listed here so they are not silently absorbed:

* `makeAggregateInfos` called `toAggregateInfo` with a synthetic `AggregationNode` whose source was
  `nullptr` — an unconditional null dereference at `AggregateInfo.cpp:49`. Replaced with hand-built
  `AggregateInfo`s.
* `advanceDrain` passed a **null** `RowVectorPtr` to `GroupingSet::getOutput`. That method writes
  into a caller-allocated vector (`extractGroups`, `GroupingSet.cpp:847-888`); it does not create
  one. Now pre-allocated per level, mirroring `HashAggregation::prepareOutput`.
* The abandoned-level pass-through forwarded a batch of the level *below*'s row type, which both
  mis-indexed the accumulator columns in `project()` and handed the next level a vector of the wrong
  type. Now reshaped by `dropDeadKeyColumn`.
* `noMoreInput()` clobbered an in-flight pressure drain, abandoning a half-consumed
  `RowContainerIterator`. Now deferred via `pendingFinalCascade_`.
* The abandon valve was applied to the grand-total level, which `HashAggregation` explicitly
  excludes (`HashAggregation.cpp:183-184` `VELOX_CHECK`s `!isGlobal_`). Now guarded.
* `resolveIntermediateType` was described as file-local and unexposed. It is public
  (`AggregateFunctionRegistry.h:58`) and is now the single source of truth for intermediate types.
* The `ExpandRel` level/column indexing was transposed (§5.1).
* `RollupNode::makeOutputType` is declared and used but **never defined** — still outstanding.
