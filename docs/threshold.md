# Threshold indicator proof of concept

The `threshold` indicator counts good one-minute checks instead of good requests.
The [checkout example](../examples/checkout-latency-threshold.yaml) requires p95 latency to stay at or below 0.4 seconds in 99% of evaluated minutes.

Exactly one source is required:

- `histogram`: `metric` selects every bucket of a classic histogram, and `quantile` selects a value between 0 and 1.
- `nativeHistogram`: the same fields, for a native histogram.
- `gauge`: `metric` selects a gauge. Each matching series produces its own check.

`op` accepts `lt`, `lte`, `gt`, or `gte`. `value` is a number in the source metric's unit.
`grouping` preserves histogram labels before calculating each group's quantile and groups gauge checks for accounting.

## One-minute checks

Pyrra generates a separate `<slo-name>-threshold` group with `interval: 1m` and `[1m]` ranges.
Each check records `pyrra_threshold{slo="<slo-name>"}` as 1 for good or 0 for bad.
The existing boolean-gauge rules count those samples and calculate burn rates and alerts.

Histogram minutes without traffic produce no check. They count as neither good nor bad.
The absent alert watches the source metric, so a quiet period alone does not trigger it.

Gauges use the last sample **within the minute**, not an average or a worst value.
A minute without samples produces no check, even if Prometheus could still find an older sample through its lookback.
This is a PoC choice because the proposal leaves gauge semantics open. There is no configurable missing-data policy.

Histogram sources require a scrape interval of 30 seconds or less because `rate()` needs at least two samples within each minute.
Validation warns for SLO windows shorter than 14 days, whose shortest alert windows contain few checks.
There is no `slice` field or longer lookback.

All rule outputs retain the separate group. With `performanceOverAccuracy`, Prometheus runs the group with the short rules.
Mimir receives a separate one-minute group alongside the ordinary 30-second group.

## PoC limits and tests

The UI and protobuf API do not expose threshold indicators yet. YAML configuration and generated rules are the supported PoC path.

With Prometheus 3 `promtool` on `PATH`, `go test ./slo -run Threshold -v` also runs rule unit tests against generated rules.
Without `promtool`, those tests skip; API and rule-generation tests still run.
The rule tests cover good, quiet, and slow histogram minutes, grouping, SLO isolation, native histograms, and all gauge operators.
`promtool` ignores individual group intervals, so the test suite explicitly uses one-minute evaluation steps and tests output intervals separately.
