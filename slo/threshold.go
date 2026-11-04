package slo

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const thresholdMetric = "pyrra_threshold"

// boolGaugeObjective points accounting queries at the checks, not their source.
// Keep Threshold so absent alerts can still watch the original metric.
func (o Objective) boolGaugeObjective() Objective {
	t := o.Indicator.Threshold
	if t == nil || o.Indicator.BoolGauge != nil {
		return o
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, thresholdMetric),
		labels.MustNewMatcher(labels.MatchEqual, "slo", o.Name()),
	}
	for _, m := range t.Metric.LabelMatchers {
		if m.Name == model.MetricNameLabel || m.Name == "slo" {
			continue
		}
		// Aggregation drops ungrouped regex labels. Equality labels are restored
		// on the check rule, while grouping labels survive the aggregation.
		if m.Type == labels.MatchEqual || slices.Contains(t.Grouping, m.Name) {
			matchers = append(matchers, m)
		}
	}
	o.Indicator.BoolGauge = &BoolGaugeIndicator{
		Metric:   Metric{Name: thresholdMetric, LabelMatchers: matchers},
		Grouping: t.Grouping,
	}
	return o
}

// ThresholdRules runs once per non-overlapping minute. Empty histogram slices
// produce no sample, so count_over_time excludes them from the denominator.
func (o Objective) ThresholdRules(opts GenerationOptions) (monitoringv1.RuleGroup, error) {
	t := o.Indicator.Threshold
	if t == nil {
		return monitoringv1.RuleGroup{}, nil
	}
	op, ok := map[string]string{"lt": "<", "lte": "<=", "gt": ">", "gte": ">="}[t.Op]
	if !ok {
		return monitoringv1.RuleGroup{}, fmt.Errorf("invalid threshold operator %q", t.Op)
	}
	metric := Metric{Name: t.Metric.Name, LabelMatchers: applyPrometheus3Migration(cloneMatchers(t.Metric.LabelMatchers), opts)}
	selector := metric.Metric()
	grouping := strings.Join(t.Grouping, ", ")
	value := ""
	traffic := ""
	switch t.Kind {
	case "histogram":
		buckets := append([]string{labels.BucketLabel}, t.Grouping...)
		value = fmt.Sprintf("histogram_quantile(%s, sum by (%s) (rate(%s[1m])))", strconv.FormatFloat(t.Quantile, 'g', -1, 64), strings.Join(buckets, ", "), selector)
		metric.LabelMatchers = append(metric.LabelMatchers, labels.MustNewMatcher(labels.MatchEqual, labels.BucketLabel, "+Inf"))
		traffic = fmt.Sprintf("sum by (%s) (rate(%s[1m])) > 0", grouping, metric.Metric())
	case "nativeHistogram":
		rate := fmt.Sprintf("sum by (%s) (rate(%s[1m]))", grouping, selector)
		value = fmt.Sprintf("histogram_quantile(%s, %s)", strconv.FormatFloat(t.Quantile, 'g', -1, 64), rate)
		traffic = fmt.Sprintf("histogram_count(%s) > 0", rate)
	case "gauge":
		// Compare the last sample within this minute, per source series.
		// Unlike an instant selector this cannot reuse a previous quiet minute.
		value = fmt.Sprintf("last_over_time(%s[1m])", selector)
	default:
		return monitoringv1.RuleGroup{}, fmt.Errorf("invalid threshold source %q", t.Kind)
	}
	query := fmt.Sprintf("%s %s bool %s", value, op, strconv.FormatFloat(t.Value, 'g', -1, 64))
	if traffic != "" {
		query = fmt.Sprintf("(%s) and (%s)", query, traffic)
	}
	expr, err := parser.NewParser(parser.Options{}).ParseExpr(query)
	if err != nil {
		return monitoringv1.RuleGroup{}, err
	}
	ruleLabels := o.commonRuleLabels(o.Name())
	for _, g := range t.Grouping {
		delete(ruleLabels, g)
	}
	for _, m := range t.Metric.LabelMatchers {
		if m.Type == labels.MatchEqual && m.Name != model.MetricNameLabel && m.Name != "slo" && !slices.Contains(t.Grouping, m.Name) {
			ruleLabels[m.Name] = m.Value
		}
	}
	return monitoringv1.RuleGroup{
		Name:     o.Name() + "-threshold",
		Interval: monitoringDuration("1m"),
		Rules: []monitoringv1.Rule{{
			Record: thresholdMetric,
			Expr:   intstr.FromString(expr.String()),
			Labels: ruleLabels,
		}},
	}, nil
}
