package slo

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func objectiveThreshold() Objective {
	return Objective{
		Labels:   labels.FromStrings("__name__", "checkout-latency"),
		Target:   0.99,
		Window:   model.Duration(28 * 24 * time.Hour),
		Alerting: Alerting{Burnrates: true, Absent: true},
		Indicator: Indicator{Threshold: &ThresholdIndicator{
			Kind: "histogram",
			Metric: Metric{Name: "request_duration_seconds_bucket", LabelMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "request_duration_seconds_bucket"),
				labels.MustNewMatcher(labels.MatchEqual, "job", "checkout"),
				labels.MustNewMatcher(labels.MatchRegexp, "instance", ".+"),
			}},
			Quantile: 0.95, Op: "lte", Value: 0.4, Grouping: []string{"route"},
		}},
	}
}

func TestThresholdRules(t *testing.T) {
	o := objectiveThreshold()
	group, err := o.ThresholdRules(GenerationOptions{})
	require.NoError(t, err)
	require.Equal(t, "checkout-latency-threshold", group.Name)
	require.Equal(t, monitoringDuration("1m"), group.Interval)
	require.Len(t, group.Rules, 1)
	require.Equal(t, "pyrra_threshold", group.Rules[0].Record)
	require.Equal(t, map[string]string{"slo": "checkout-latency", "job": "checkout"}, group.Rules[0].Labels)
	require.Equal(t, `(histogram_quantile(0.95, sum by (le, route) (rate(request_duration_seconds_bucket{instance=~".+",job="checkout"}[1m]))) <= bool 0.4) and (sum by (route) (rate(request_duration_seconds_bucket{instance=~".+",job="checkout",le="+Inf"}[1m])) > 0)`, group.Rules[0].Expr.String())

	for _, performance := range []bool{false, true} {
		o.PerformanceOverAccuracy = performance
		increase, err := o.IncreaseRules(GenerationOptions{})
		require.NoError(t, err)
		for _, r := range increase.Rules {
			_, err := parser.NewParser(parser.Options{}).ParseExpr(r.Expr.String())
			require.NoError(t, err)
			if r.Alert != "" {
				require.Equal(t, `absent(request_duration_seconds_bucket{instance=~".+",job="checkout"}) == 1`, r.Expr.String())
				require.Equal(t, "checkout-latency", r.Labels["slo"])
			} else {
				require.Contains(t, r.Expr.String(), `slo="checkout-latency"`)
				require.NotContains(t, r.Expr.String(), "request_duration_seconds_bucket")
				require.NotContains(t, r.Expr.String(), "instance")
			}
		}
	}
	require.Equal(t, Threshold, o.IndicatorType())
	require.Nil(t, o.Indicator.BoolGauge, "generating rules must not mutate the objective")
}

// Exercise generated rules and stale markers at one-minute steps, rather than
// maintaining a hand-written copy of the expression.
func TestThresholdPromtool(t *testing.T) {
	o := objectiveThreshold()
	threshold, err := o.ThresholdRules(GenerationOptions{})
	require.NoError(t, err)
	increases, err := o.IncreaseRules(GenerationOptions{})
	require.NoError(t, err)
	burnrates, err := o.Burnrates(GenerationOptions{})
	require.NoError(t, err)
	tests, err := os.ReadFile("testdata/threshold.test.yaml")
	require.NoError(t, err)
	runThresholdPromtool(t, []monitoringv1.RuleGroup{threshold, increases, burnrates}, string(tests))
}

func TestThresholdNativeHistogram(t *testing.T) {
	o := objectiveThreshold()
	o.Indicator.Threshold.Kind = "nativeHistogram"
	o.Indicator.Threshold.Metric = Metric{Name: "duration"}
	o.Indicator.Threshold.Value = 1
	group, err := o.ThresholdRules(GenerationOptions{})
	require.NoError(t, err)
	runThresholdPromtool(t, []monitoringv1.RuleGroup{group}, `
tests:
- interval: 15s
  input_series:
  - series: 'duration{route="fast"}'
    values: '{{schema:0 sum:1 count:10 buckets:[10] offset:-2}}+{{schema:0 sum:1 count:10 buckets:[10] offset:-2}}x8'
  - series: 'duration{route="slow"}'
    values: '{{schema:0 sum:60 count:10 buckets:[10] offset:3}}+{{schema:0 sum:60 count:10 buckets:[10] offset:3}}x8'
  - series: 'duration{route="quiet"}'
    values: '{{schema:0 sum:0 count:0}}x8'
  promql_expr_test:
  - expr: pyrra_threshold
    eval_time: 1m
    exp_samples:
    - labels: 'pyrra_threshold{slo="checkout-latency",route="fast"}'
      value: 1
    - labels: 'pyrra_threshold{slo="checkout-latency",route="slow"}'
      value: 0
`)
}

func TestThresholdGaugeOperators(t *testing.T) {
	for _, tc := range []struct {
		op               string
		low, equal, high int
	}{
		{"lt", 1, 0, 0},
		{"lte", 1, 1, 0},
		{"gt", 0, 0, 1},
		{"gte", 0, 1, 1},
	} {
		t.Run(tc.op, func(t *testing.T) {
			o := objectiveThreshold()
			o.Indicator.Threshold = &ThresholdIndicator{Kind: "gauge", Metric: Metric{Name: "lag"}, Op: tc.op, Value: 10}
			group, err := o.ThresholdRules(GenerationOptions{})
			require.NoError(t, err)
			runThresholdPromtool(t, []monitoringv1.RuleGroup{group}, fmt.Sprintf(`
tests:
- interval: 15s
  input_series:
  - series: 'lag{instance="low"}'
    values: '100 100 100 9'
  - series: 'lag{instance="equal"}'
    values: '0 0 0 10'
  - series: 'lag{instance="high"}'
    values: '0 0 0 11'
  promql_expr_test:
  - expr: pyrra_threshold
    eval_time: 1m
    exp_samples:
    - labels: 'pyrra_threshold{slo="checkout-latency",instance="low"}'
      value: %d
    - labels: 'pyrra_threshold{slo="checkout-latency",instance="equal"}'
      value: %d
    - labels: 'pyrra_threshold{slo="checkout-latency",instance="high"}'
      value: %d
  - expr: pyrra_threshold
    eval_time: 2m
    exp_samples: []
`, tc.low, tc.equal, tc.high))
		})
	}
}

func TestThresholdBurnrate(t *testing.T) {
	o := objectiveThreshold()
	query := o.Burnrate(5*time.Minute, GenerationOptions{})
	require.Contains(t, query, `slo="checkout-latency"`)
	require.Contains(t, query, "count_over_time")
	require.Contains(t, query, "sum_over_time")
	_, err := o.QueryBurnrate(5*time.Minute, nil)
	require.NoError(t, err)
	for _, q := range []string{o.QueryTotal(o.Window, GenerationOptions{}), o.QueryErrors(o.Window, GenerationOptions{}), o.QueryErrorBudget(GenerationOptions{})} {
		require.Contains(t, q, "pyrra_threshold:")
		_, err := parser.NewParser(parser.Options{}).ParseExpr(q)
		require.NoError(t, err)
	}
}

func runThresholdPromtool(t *testing.T, groups []monitoringv1.RuleGroup, tests string) {
	t.Helper()
	promtool, err := exec.LookPath("promtool")
	if err != nil {
		t.Skip("install promtool and add it to PATH to run rule unit tests")
	}
	dir := t.TempDir()
	rules, err := yaml.Marshal(monitoringv1.PrometheusRuleSpec{Groups: groups})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "rules.yaml"), rules, 0o600))
	// promtool evaluates every group at evaluation_interval, ignoring each
	// group's interval. Assert those separately and evaluate the checks at 1m.
	header := "rule_files: [rules.yaml]\nevaluation_interval: 1m\ngroup_eval_order:\n"
	for _, group := range groups {
		header += "- " + group.Name + "\n"
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "tests.yaml"), []byte(header+tests), 0o600))
	cmd := exec.Command(promtool, "test", "rules", "tests.yaml")
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
	t.Logf("%s", output)
}
