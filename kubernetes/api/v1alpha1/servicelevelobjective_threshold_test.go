package v1alpha1_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/pyrra-dev/pyrra/kubernetes/api/v1alpha1"
)

func thresholdObjective(kind string) *v1alpha1.ServiceLevelObjective {
	threshold := &v1alpha1.ThresholdIndicator{Op: "lte", Value: new(0.4), Grouping: []string{"job"}}
	switch kind {
	case "histogram":
		threshold.Histogram = &v1alpha1.ThresholdHistogram{
			Query: v1alpha1.Query{Metric: `request_duration_seconds_bucket{job="checkout"}`}, Quantile: new(0.95),
		}
	case "nativeHistogram":
		threshold.NativeHistogram = &v1alpha1.ThresholdHistogram{
			Query: v1alpha1.Query{Metric: `request_duration_seconds{job="checkout"}`}, Quantile: new(0.95),
		}
	case "gauge":
		threshold.Gauge = &v1alpha1.Query{Metric: `queue_lag{job="checkout"}`}
	}
	return &v1alpha1.ServiceLevelObjective{
		ObjectMeta: metav1.ObjectMeta{Name: "checkout", Namespace: "monitoring"},
		Spec: v1alpha1.ServiceLevelObjectiveSpec{
			Target: "99", Window: "28d",
			ServiceLevelIndicator: v1alpha1.ServiceLevelIndicator{Threshold: threshold},
		},
	}
}

func TestServiceLevelObjective_ThresholdValidation(t *testing.T) {
	ctx := context.Background()
	// Every invalid case must also fail Internal: filesystem inputs do not use the webhook.
	check := func(t *testing.T, objective *v1alpha1.ServiceLevelObjective, wantError string) {
		t.Helper()
		warnings, err := objective.ValidateCreate(ctx, objective)
		require.Empty(t, warnings)
		_, updateErr := objective.ValidateUpdate(ctx, nil, objective)
		_, internalErr := objective.Internal()
		for _, got := range []error{err, updateErr, internalErr} {
			if wantError == "" {
				require.NoError(t, got)
			} else {
				require.ErrorContains(t, got, wantError)
			}
		}
	}

	for sources := 0; sources < 8; sources++ {
		t.Run(fmt.Sprintf("sources/%d", sources), func(t *testing.T) {
			objective := thresholdObjective("none")
			threshold := objective.Spec.ServiceLevelIndicator.Threshold
			if sources&1 != 0 {
				threshold.Histogram = &v1alpha1.ThresholdHistogram{Query: v1alpha1.Query{Metric: "duration_bucket"}, Quantile: new(0.95)}
			}
			if sources&2 != 0 {
				threshold.NativeHistogram = &v1alpha1.ThresholdHistogram{Query: v1alpha1.Query{Metric: "duration"}, Quantile: new(0.95)}
			}
			if sources&4 != 0 {
				threshold.Gauge = &v1alpha1.Query{Metric: "lag"}
			}
			wantError := "threshold must set exactly one"
			if sources == 1 || sources == 2 || sources == 4 {
				wantError = ""
			}
			check(t, objective, wantError)
		})
	}

	for _, indicator := range []string{"ratio", "latency", "latencyNative", "bool_gauge"} {
		t.Run("exclusive/"+indicator, func(t *testing.T) {
			objective := thresholdObjective("gauge")
			switch indicator {
			case "ratio":
				objective.Spec.ServiceLevelIndicator.Ratio = &v1alpha1.RatioIndicator{}
			case "latency":
				objective.Spec.ServiceLevelIndicator.Latency = &v1alpha1.LatencyIndicator{}
			case "latencyNative":
				objective.Spec.ServiceLevelIndicator.LatencyNative = &v1alpha1.NativeLatencyIndicator{}
			case "bool_gauge":
				objective.Spec.ServiceLevelIndicator.BoolGauge = &v1alpha1.BoolGaugeIndicator{}
			}
			check(t, objective, "threshold cannot be combined")
		})
	}

	for _, op := range []string{"lt", "lte", "gt", "gte", "", "<=", "eq", "LTE"} {
		t.Run("op/"+op, func(t *testing.T) {
			objective := thresholdObjective("gauge")
			objective.Spec.ServiceLevelIndicator.Threshold.Op = op
			wantError := ""
			if op != "lt" && op != "lte" && op != "gt" && op != "gte" {
				wantError = "threshold op must be one of"
			}
			check(t, objective, wantError)
		})
	}

	for _, kind := range []string{"histogram", "nativeHistogram", "gauge"} {
		t.Run(kind, func(t *testing.T) {
			for _, value := range []float64{0, -0.4, 123, math.NaN(), math.Inf(1), math.Inf(-1)} {
				t.Run(fmt.Sprintf("value/%v", value), func(t *testing.T) {
					objective := thresholdObjective(kind)
					objective.Spec.ServiceLevelIndicator.Threshold.Value = new(value)
					wantError := ""
					if math.IsNaN(value) || math.IsInf(value, 0) {
						wantError = "threshold value must be finite"
					}
					check(t, objective, wantError)
				})
			}
			if kind != "gauge" {
				for _, quantile := range []float64{0, 0.95, 1, -0.01, 1.01, math.NaN(), math.Inf(1), math.Inf(-1)} {
					t.Run(fmt.Sprintf("quantile/%v", quantile), func(t *testing.T) {
						objective := thresholdObjective(kind)
						threshold := objective.Spec.ServiceLevelIndicator.Threshold
						if kind == "histogram" {
							threshold.Histogram.Quantile = new(quantile)
						} else {
							threshold.NativeHistogram.Quantile = new(quantile)
						}
						wantError := ""
						if math.IsNaN(quantile) || math.IsInf(quantile, 0) || quantile < 0 || quantile > 1 {
							wantError = "quantile must be finite and between 0 and 1"
						}
						check(t, objective, wantError)
					})
				}
			}
			for _, tc := range []struct{ metric, wantError string }{
				{"", "metric must be set"},
				{"foo{", "failed to parse threshold"},
				{"sum(foo)", "must be a vector selector"},
				{"rate(foo[1m])", "must be a vector selector"},
				{"foo / bar", "must be a vector selector"},
				{"foo[1m]", "must be a vector selector"},
				{"1", "must be a vector selector"},
				{"foo offset 1m", "must not use offset or @ modifiers"},
				{"foo @ 123", "must not use offset or @ modifiers"},
				{"foo @ start()", "must not use offset or @ modifiers"},
				{`foo{slo="other"}`, "reserved slo label"},
				{`foo{job=~"checkout|cart"}`, ""},
				{`{__name__="foo",job="checkout"}`, ""},
			} {
				t.Run("metric/"+tc.metric, func(t *testing.T) {
					objective := thresholdObjective(kind)
					threshold := objective.Spec.ServiceLevelIndicator.Threshold
					switch kind {
					case "histogram":
						threshold.Histogram.Metric = tc.metric
					case "nativeHistogram":
						threshold.NativeHistogram.Metric = tc.metric
					case "gauge":
						threshold.Gauge.Metric = tc.metric
					}
					check(t, objective, tc.wantError)
				})
			}
			for _, tc := range []struct {
				grouping  []string
				wantError string
			}{
				{nil, ""},
				{[]string{"job", "route"}, ""},
				{[]string{"_zone", "zone2"}, ""},
				{[]string{""}, "is invalid"},
				{[]string{"not-valid"}, "is invalid"},
				{[]string{"1zone"}, "is invalid"},
				{[]string{"a.b"}, "is invalid"},
				{[]string{"slo"}, "is reserved"},
				{[]string{"__name__"}, "is reserved"},
				{[]string{"job", "job"}, "is repeated"},
			} {
				t.Run(fmt.Sprintf("grouping/%v", tc.grouping), func(t *testing.T) {
					objective := thresholdObjective(kind)
					objective.Spec.ServiceLevelIndicator.Threshold.Grouping = tc.grouping
					check(t, objective, tc.wantError)
				})
			}
			t.Run("grouping/le", func(t *testing.T) {
				objective := thresholdObjective(kind)
				objective.Spec.ServiceLevelIndicator.Threshold.Grouping = []string{"le"}
				wantError := ""
				if kind == "histogram" {
					wantError = "is reserved"
				}
				check(t, objective, wantError)
			})
		})
	}

	for _, matcher := range []string{`le="0.4"`, `le!="0.4"`, `le=~".*"`, `le!~"0.4"`} {
		t.Run("wholeHistogram/"+matcher, func(t *testing.T) {
			objective := thresholdObjective("histogram")
			objective.Spec.ServiceLevelIndicator.Threshold.Histogram.Metric = "duration_bucket{" + matcher + "}"
			check(t, objective, "whole histogram is required")
		})
	}
}

func TestServiceLevelObjective_ThresholdRequiredNumbers(t *testing.T) {
	for _, kind := range []string{"histogram", "nativeHistogram", "gauge"} {
		for _, tc := range []struct{ name, value, quantile, wantError string }{
			{"explicitZero", "0", "0", ""},
			{"missingValue", "", "0", "threshold value must be set"},
			{"nullValue", "null", "0", "threshold value must be set"},
			{"missingQuantile", "0", "", "quantile must be set"},
			{"nullQuantile", "0", "null", "quantile must be set"},
		} {
			if kind == "gauge" && tc.wantError == "quantile must be set" {
				continue
			}
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				value, quantile := "", ""
				if tc.value != "" {
					value = "      value: " + tc.value + "\n"
				}
				if kind != "gauge" && tc.quantile != "" {
					quantile = "        quantile: " + tc.quantile + "\n"
				}
				config := fmt.Sprintf(`metadata: {name: checkout, namespace: monitoring}
spec:
  target: '99'
  window: 28d
  indicator:
    threshold:
      %s:
        metric: duration
%s      op: lte
%s`, kind, quantile, value)
				var objective v1alpha1.ServiceLevelObjective
				require.NoError(t, yaml.UnmarshalStrict([]byte(config), &objective))
				_, createErr := objective.ValidateCreate(context.Background(), &objective)
				_, updateErr := objective.ValidateUpdate(context.Background(), nil, &objective)
				internal, internalErr := objective.Internal()
				for _, err := range []error{createErr, updateErr, internalErr} {
					if tc.wantError == "" {
						require.NoError(t, err)
					} else {
						require.ErrorContains(t, err, tc.wantError)
					}
				}
				if tc.wantError == "" {
					require.NotNil(t, objective.Spec.ServiceLevelIndicator.Threshold.Value)
					require.Zero(t, internal.Indicator.Threshold.Value)
					require.Zero(t, internal.Indicator.Threshold.Quantile)
					encoded, err := json.Marshal(objective)
					require.NoError(t, err)
					require.Contains(t, string(encoded), `"value":0`)
					if kind != "gauge" {
						require.Contains(t, string(encoded), `"quantile":0`)
					}
				}
			})
		}
	}
}

func TestServiceLevelObjective_ThresholdWindowWarning(t *testing.T) {
	for _, window := range []string{"7d", "13d23h59m", "14d", "28d"} {
		t.Run(window, func(t *testing.T) {
			objective := thresholdObjective("gauge")
			objective.Spec.Window = window
			warnings, err := objective.ValidateCreate(context.Background(), objective)
			require.NoError(t, err)
			if window == "7d" || window == "13d23h59m" {
				require.Len(t, warnings, 1)
				require.Contains(t, warnings[0], "at least 14d")
			} else {
				require.Empty(t, warnings)
			}
		})
	}
}

func TestServiceLevelObjective_ThresholdInternal(t *testing.T) {
	for _, kind := range []string{"histogram", "nativeHistogram", "gauge"} {
		t.Run(kind, func(t *testing.T) {
			quantile := "        quantile: 0.95\n"
			if kind == "gauge" {
				quantile = ""
			}
			config := fmt.Sprintf(`apiVersion: pyrra.dev/v1alpha1
kind: ServiceLevelObjective
metadata:
  name: checkout
  namespace: monitoring
spec:
  target: '99'
  window: 28d
  indicator:
    threshold:
      %s:
        metric: duration{job="checkout",route=~"cart|checkout",region!="test"}
%s      op: lte
      value: 0.4
      grouping: [job, route]
`, kind, quantile)
			var objective v1alpha1.ServiceLevelObjective
			require.NoError(t, yaml.UnmarshalStrict([]byte(config), &objective))
			internal, err := objective.Internal()
			require.NoError(t, err)
			require.Nil(t, internal.Indicator.BoolGauge, "normalization belongs to slo methods, not API conversion")
			require.Nil(t, internal.Indicator.Ratio)
			require.Nil(t, internal.Indicator.Latency)
			require.Nil(t, internal.Indicator.LatencyNative)
			threshold := internal.Indicator.Threshold
			require.NotNil(t, threshold)
			require.Equal(t, kind, threshold.Kind)
			require.Equal(t, "lte", threshold.Op)
			require.Equal(t, 0.4, threshold.Value)
			if kind == "gauge" {
				require.Zero(t, threshold.Quantile)
			} else {
				require.Equal(t, 0.95, threshold.Quantile)
			}
			require.Equal(t, []string{"job", "route"}, threshold.Grouping)
			require.Equal(t, "duration", threshold.Metric.Name)
			require.Len(t, threshold.Metric.LabelMatchers, 4)
			for _, matcher := range threshold.Metric.LabelMatchers {
				if matcher.Name == "route" {
					require.Equal(t, labels.MatchRegexp, matcher.Type)
					require.True(t, matcher.Matches("cart"))
					require.False(t, matcher.Matches("other"))
				}
			}
			var roundTrip v1alpha1.ServiceLevelObjective
			require.NoError(t, yaml.UnmarshalStrict([]byte(internal.Config), &roundTrip))
			require.Equal(t, objective.Spec.ServiceLevelIndicator.Threshold, roundTrip.Spec.ServiceLevelIndicator.Threshold)
			encoded, err := json.Marshal(objective)
			require.NoError(t, err)
			var decoded map[string]any
			require.NoError(t, json.Unmarshal(encoded, &decoded))
			thresholdJSON := decoded["spec"].(map[string]any)["indicator"].(map[string]any)["threshold"].(map[string]any)
			require.Equal(t, 0.4, thresholdJSON["value"], "value must be a JSON number, not a string")
			if kind != "gauge" {
				require.Equal(t, 0.95, thresholdJSON[kind].(map[string]any)["quantile"])
			}
			objective.Spec.ServiceLevelIndicator.Threshold.Grouping[0] = "changed"
			require.Equal(t, []string{"job", "route"}, threshold.Grouping, "Internal must preserve its own grouping")
		})
	}
}
