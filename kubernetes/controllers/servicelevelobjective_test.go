package controllers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/stretchr/testify/require"
	yamlv3 "gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	pyrrav1alpha1 "github.com/pyrra-dev/pyrra/kubernetes/api/v1alpha1"
	"github.com/pyrra-dev/pyrra/mimir"
	"github.com/pyrra-dev/pyrra/slo"
)

var (
	trueBool = true
	httpSLO  = pyrrav1alpha1.ServiceLevelObjective{
		TypeMeta: metav1.TypeMeta{
			APIVersion: pyrrav1alpha1.GroupVersion.Version,
			Kind:       "ServiceLevelObjective",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "http",
			UID:  "123",
			Labels: map[string]string{
				slo.PropagationLabelsPrefix + "team": "foo",
				"team":                               "bar",
			},
			Annotations: map[string]string{
				slo.PropagationLabelsPrefix + "description": "foo",
				"description": "bar",
			},
		},
		Spec: pyrrav1alpha1.ServiceLevelObjectiveSpec{
			Target: "99.5",
			Window: "28d",
			ServiceLevelIndicator: pyrrav1alpha1.ServiceLevelIndicator{
				Ratio: &pyrrav1alpha1.RatioIndicator{
					Errors: pyrrav1alpha1.Query{
						Metric: `http_requests_total{job="app",status=~"5.."}`,
					},
					Total: pyrrav1alpha1.Query{
						Metric: `http_requests_total{job="app"}`,
					},
				},
			},
			PartialResponseStrategy: "warn",
		},
	}
)

func Test_makePrometheusRule(t *testing.T) {
	tests := []struct {
		name      string
		objective pyrrav1alpha1.ServiceLevelObjective
		rules     *monitoringv1.PrometheusRule
	}{
		{
			name:      "http",
			objective: httpSLO,
			rules: &monitoringv1.PrometheusRule{
				TypeMeta: metav1.TypeMeta{
					APIVersion: monitoring.GroupName + "/" + monitoringv1.Version,
					Kind:       monitoringv1.PrometheusRuleKind,
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "http",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: pyrrav1alpha1.GroupVersion.Version,
							Kind:       "ServiceLevelObjective",
							Name:       "http",
							UID:        "123",
							Controller: &trueBool,
						},
					},
					Labels: map[string]string{
						"pyrra.dev/team": "foo",
						"team":           "bar",
					},
				},
				Spec: monitoringv1.PrometheusRuleSpec{
					Groups: []monitoringv1.RuleGroup{
						{
							Name:                    "http-increase",
							PartialResponseStrategy: "warn",
							Interval:                monitoringDuration("2m30s"),
							Rules: []monitoringv1.Rule{
								{
									Record: "http_requests:increase4w",
									Expr:   intstr.FromString(`sum by (status) (increase(http_requests_total{job="app"}[4w]))`),
									Labels: map[string]string{
										"job":  "app",
										"slo":  "http",
										"team": "foo",
									},
								},
								{
									Alert: "SLOMetricAbsent",
									Expr:  intstr.FromString(`absent(http_requests_total{job="app"}) == 1`),
									For:   monitoringDuration("6m"),
									Annotations: map[string]string{
										"description": "foo",
									},
									Labels: map[string]string{
										"severity": "critical",
										"job":      "app",
										"slo":      "http",
										"team":     "foo",
									},
								},
							},
						},
						{
							Name:                    "http",
							PartialResponseStrategy: "warn",
							Interval:                monitoringDuration("30s"),
							Rules: []monitoringv1.Rule{
								{
									Record: "http_requests:burnrate5m",
									Expr:   intstr.FromString(`sum(rate(http_requests_total{job="app",status=~"5.."}[5m])) / sum(rate(http_requests_total{job="app"}[5m]))`),
									Labels: map[string]string{"job": "app", "slo": "http", "team": "foo"},
								},
								{
									Record: "http_requests:burnrate30m",
									Expr:   intstr.FromString(`sum(rate(http_requests_total{job="app",status=~"5.."}[30m])) / sum(rate(http_requests_total{job="app"}[30m]))`),
									Labels: map[string]string{"job": "app", "slo": "http", "team": "foo"},
								},
								{
									Record: "http_requests:burnrate1h",
									Expr:   intstr.FromString(`sum(rate(http_requests_total{job="app",status=~"5.."}[1h])) / sum(rate(http_requests_total{job="app"}[1h]))`),
									Labels: map[string]string{"job": "app", "slo": "http", "team": "foo"},
								},
								{
									Record: "http_requests:burnrate2h",
									Expr:   intstr.FromString(`sum(rate(http_requests_total{job="app",status=~"5.."}[2h])) / sum(rate(http_requests_total{job="app"}[2h]))`),
									Labels: map[string]string{"job": "app", "slo": "http", "team": "foo"},
								},
								{
									Record: "http_requests:burnrate6h",
									Expr:   intstr.FromString(`sum(rate(http_requests_total{job="app",status=~"5.."}[6h])) / sum(rate(http_requests_total{job="app"}[6h]))`),
									Labels: map[string]string{"job": "app", "slo": "http", "team": "foo"},
								},
								{
									Record: "http_requests:burnrate1d",
									Expr:   intstr.FromString(`sum(rate(http_requests_total{job="app",status=~"5.."}[1d])) / sum(rate(http_requests_total{job="app"}[1d]))`),
									Labels: map[string]string{"job": "app", "slo": "http", "team": "foo"},
								},
								{
									Record: "http_requests:burnrate4d",
									Expr:   intstr.FromString(`sum(rate(http_requests_total{job="app",status=~"5.."}[4d])) / sum(rate(http_requests_total{job="app"}[4d]))`),
									Labels: map[string]string{"job": "app", "slo": "http", "team": "foo"},
								},
								{
									Alert:       "ErrorBudgetBurn",
									Expr:        intstr.FromString(`http_requests:burnrate5m{job="app",slo="http"} > (14 * (1-0.995)) and http_requests:burnrate1h{job="app",slo="http"} > (14 * (1-0.995))`),
									For:         monitoringDuration("2m0s"),
									Labels:      map[string]string{"severity": "critical", "job": "app", "long": "1h", "slo": "http", "short": "5m", "team": "foo", "exhaustion": "2d"},
									Annotations: map[string]string{"description": "foo"},
								},
								{
									Alert:       "ErrorBudgetBurn",
									Expr:        intstr.FromString(`http_requests:burnrate30m{job="app",slo="http"} > (7 * (1-0.995)) and http_requests:burnrate6h{job="app",slo="http"} > (7 * (1-0.995))`),
									For:         monitoringDuration("15m0s"),
									Labels:      map[string]string{"severity": "critical", "job": "app", "long": "6h", "slo": "http", "short": "30m", "team": "foo", "exhaustion": "4d"},
									Annotations: map[string]string{"description": "foo"},
								},
								{
									Alert:       "ErrorBudgetBurn",
									Expr:        intstr.FromString(`http_requests:burnrate2h{job="app",slo="http"} > (2 * (1-0.995)) and http_requests:burnrate1d{job="app",slo="http"} > (2 * (1-0.995))`),
									For:         monitoringDuration("1h0m0s"),
									Labels:      map[string]string{"severity": "warning", "job": "app", "long": "1d", "slo": "http", "short": "2h", "team": "foo", "exhaustion": "2w"},
									Annotations: map[string]string{"description": "foo"},
								},
								{
									Alert:       "ErrorBudgetBurn",
									Expr:        intstr.FromString(`http_requests:burnrate6h{job="app",slo="http"} > (1 * (1-0.995)) and http_requests:burnrate4d{job="app",slo="http"} > (1 * (1-0.995))`),
									For:         monitoringDuration("3h0m0s"),
									Labels:      map[string]string{"severity": "warning", "job": "app", "long": "4d", "slo": "http", "short": "6h", "team": "foo", "exhaustion": "4w"},
									Annotations: map[string]string{"description": "foo"},
								},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prometheusRule, err := makePrometheusRule(tt.objective, false, false, "")
			require.NoError(t, err)
			require.Equal(t, tt.rules, prometheusRule)
		})
	}
}

func Test_makeSplitPrometheusRules(t *testing.T) {
	perfSLO := httpSLO.DeepCopy()
	perfSLO.Spec.PerformanceOverAccuracy = true
	perfSLO.Spec.RuleOutput = &pyrrav1alpha1.RuleOutput{
		ShortRulesLabels: map[string]string{"prometheus": "k8s"},
		LongRulesLabels:  map[string]string{"prometheus": "thanos-k8s"},
	}

	shortRule, longRule, err := makeSplitPrometheusRules(*perfSLO, false, false, "")
	require.NoError(t, err)

	expectedShortRule := &monitoringv1.PrometheusRule{
		TypeMeta: metav1.TypeMeta{
			APIVersion: monitoring.GroupName + "/" + monitoringv1.Version,
			Kind:       monitoringv1.PrometheusRuleKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "http-short",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: pyrrav1alpha1.GroupVersion.Version,
					Kind:       "ServiceLevelObjective",
					Name:       "http",
					UID:        "123",
					Controller: &trueBool,
				},
			},
			Labels: map[string]string{
				"pyrra.dev/team": "foo",
				"team":           "bar",
				"prometheus":     "k8s",
			},
		},
		Spec: monitoringv1.PrometheusRuleSpec{
			Groups: []monitoringv1.RuleGroup{
				{
					Name:                    "http-increase",
					PartialResponseStrategy: "warn",
					Interval:                monitoringDuration("30s"),
					Rules: []monitoringv1.Rule{
						{
							Record: "http_requests:increase5m",
							Expr:   intstr.FromString(`sum by (status) (increase(http_requests_total{job="app"}[5m]))`),
							Labels: map[string]string{
								"job":  "app",
								"slo":  "http",
								"team": "foo",
							},
						},
						{
							Alert: "SLOMetricAbsent",
							Expr:  intstr.FromString(`absent(http_requests_total{job="app"}) == 1`),
							For:   monitoringDuration("6m"),
							Annotations: map[string]string{
								"description": "foo",
							},
							Labels: map[string]string{
								"job":      "app",
								"severity": "critical",
								"slo":      "http",
								"team":     "foo",
							},
						},
					},
				},
				{
					Name:                    "http",
					PartialResponseStrategy: "warn",
					Interval:                monitoringDuration("30s"),
					Rules: []monitoringv1.Rule{
						{
							Record: "http_requests:burnrate5m",
							Expr:   intstr.FromString(`sum(rate(http_requests_total{job="app",status=~"5.."}[5m])) / sum(rate(http_requests_total{job="app"}[5m]))`),
							Labels: map[string]string{"job": "app", "slo": "http", "team": "foo"},
						},
						{
							Record: "http_requests:burnrate30m",
							Expr:   intstr.FromString(`sum(rate(http_requests_total{job="app",status=~"5.."}[30m])) / sum(rate(http_requests_total{job="app"}[30m]))`),
							Labels: map[string]string{"job": "app", "slo": "http", "team": "foo"},
						},
						{
							Record: "http_requests:burnrate1h",
							Expr:   intstr.FromString(`sum(rate(http_requests_total{job="app",status=~"5.."}[1h])) / sum(rate(http_requests_total{job="app"}[1h]))`),
							Labels: map[string]string{"job": "app", "slo": "http", "team": "foo"},
						},
						{
							Record: "http_requests:burnrate2h",
							Expr:   intstr.FromString(`sum(rate(http_requests_total{job="app",status=~"5.."}[2h])) / sum(rate(http_requests_total{job="app"}[2h]))`),
							Labels: map[string]string{"job": "app", "slo": "http", "team": "foo"},
						},
						{
							Record: "http_requests:burnrate6h",
							Expr:   intstr.FromString(`sum(rate(http_requests_total{job="app",status=~"5.."}[6h])) / sum(rate(http_requests_total{job="app"}[6h]))`),
							Labels: map[string]string{"job": "app", "slo": "http", "team": "foo"},
						},
						{
							Record: "http_requests:burnrate1d",
							Expr:   intstr.FromString(`sum(rate(http_requests_total{job="app",status=~"5.."}[1d])) / sum(rate(http_requests_total{job="app"}[1d]))`),
							Labels: map[string]string{"job": "app", "slo": "http", "team": "foo"},
						},
						{
							Record: "http_requests:burnrate4d",
							Expr:   intstr.FromString(`sum(rate(http_requests_total{job="app",status=~"5.."}[4d])) / sum(rate(http_requests_total{job="app"}[4d]))`),
							Labels: map[string]string{"job": "app", "slo": "http", "team": "foo"},
						},
						{
							Alert:       "ErrorBudgetBurn",
							Expr:        intstr.FromString(`http_requests:burnrate5m{job="app",slo="http"} > (14 * (1-0.995)) and http_requests:burnrate1h{job="app",slo="http"} > (14 * (1-0.995))`),
							For:         monitoringDuration("2m0s"),
							Labels:      map[string]string{"severity": "critical", "job": "app", "long": "1h", "slo": "http", "short": "5m", "team": "foo", "exhaustion": "2d"},
							Annotations: map[string]string{"description": "foo"},
						},
						{
							Alert:       "ErrorBudgetBurn",
							Expr:        intstr.FromString(`http_requests:burnrate30m{job="app",slo="http"} > (7 * (1-0.995)) and http_requests:burnrate6h{job="app",slo="http"} > (7 * (1-0.995))`),
							For:         monitoringDuration("15m0s"),
							Labels:      map[string]string{"severity": "critical", "job": "app", "long": "6h", "slo": "http", "short": "30m", "team": "foo", "exhaustion": "4d"},
							Annotations: map[string]string{"description": "foo"},
						},
						{
							Alert:       "ErrorBudgetBurn",
							Expr:        intstr.FromString(`http_requests:burnrate2h{job="app",slo="http"} > (2 * (1-0.995)) and http_requests:burnrate1d{job="app",slo="http"} > (2 * (1-0.995))`),
							For:         monitoringDuration("1h0m0s"),
							Labels:      map[string]string{"severity": "warning", "job": "app", "long": "1d", "slo": "http", "short": "2h", "team": "foo", "exhaustion": "2w"},
							Annotations: map[string]string{"description": "foo"},
						},
						{
							Alert:       "ErrorBudgetBurn",
							Expr:        intstr.FromString(`http_requests:burnrate6h{job="app",slo="http"} > (1 * (1-0.995)) and http_requests:burnrate4d{job="app",slo="http"} > (1 * (1-0.995))`),
							For:         monitoringDuration("3h0m0s"),
							Labels:      map[string]string{"severity": "warning", "job": "app", "long": "4d", "slo": "http", "short": "6h", "team": "foo", "exhaustion": "4w"},
							Annotations: map[string]string{"description": "foo"},
						},
					},
				},
			},
		},
	}

	expectedLongRule := &monitoringv1.PrometheusRule{
		TypeMeta: metav1.TypeMeta{
			APIVersion: monitoring.GroupName + "/" + monitoringv1.Version,
			Kind:       monitoringv1.PrometheusRuleKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "http-long",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: pyrrav1alpha1.GroupVersion.Version,
					Kind:       "ServiceLevelObjective",
					Name:       "http",
					UID:        "123",
					Controller: &trueBool,
				},
			},
			Labels: map[string]string{
				"pyrra.dev/team": "foo",
				"team":           "bar",
				"prometheus":     "thanos-k8s",
			},
		},
		Spec: monitoringv1.PrometheusRuleSpec{
			Groups: []monitoringv1.RuleGroup{
				{
					Name:                    "http-increase",
					PartialResponseStrategy: "warn",
					Interval:                monitoringDuration("2m30s"),
					Rules: []monitoringv1.Rule{
						{
							Record: "http_requests:increase4w",
							Expr:   intstr.FromString(`sum by (status) (sum_over_time(http_requests:increase5m{job="app"}[4w:5m]))`),
							Labels: map[string]string{
								"job":  "app",
								"slo":  "http",
								"team": "foo",
							},
						},
					},
				},
			},
		},
	}

	require.Equal(t, expectedShortRule, shortRule)
	require.Equal(t, expectedLongRule, longRule)
}

func Test_makeSplitPrometheusRulesWithoutRuleOutput(t *testing.T) {
	perfSLO := httpSLO.DeepCopy()
	perfSLO.Spec.PerformanceOverAccuracy = true
	// No RuleOutput set — both should inherit SLO labels

	shortRule, longRule, err := makeSplitPrometheusRules(*perfSLO, false, false, "")
	require.NoError(t, err)

	// Both should have the SLO's labels
	require.Equal(t, "foo", shortRule.Labels[slo.PropagationLabelsPrefix+"team"])
	require.Equal(t, "bar", shortRule.Labels["team"])
	require.Equal(t, "foo", longRule.Labels[slo.PropagationLabelsPrefix+"team"])
	require.Equal(t, "bar", longRule.Labels["team"])
}

func Test_mergeLabels(t *testing.T) {
	base := map[string]string{"a": "1", "b": "2"}
	override := map[string]string{"b": "3", "c": "4"}
	result := mergeLabels(base, override)
	require.Equal(t, map[string]string{"a": "1", "b": "3", "c": "4"}, result)

	// nil override should preserve base
	result = mergeLabels(base, nil)
	require.Equal(t, map[string]string{"a": "1", "b": "2"}, result)
}

func Test_makeConfigMap(t *testing.T) {
	rules := `groups:
- interval: 2m30s
  name: http-increase
  partial_response_strategy: warn
  rules:
  - expr: sum by (status) (increase(http_requests_total{job="app"}[4w]))
    labels:
      job: app
      slo: http
      team: foo
    record: http_requests:increase4w
  - alert: SLOMetricAbsent
    annotations:
      description: foo
    expr: absent(http_requests_total{job="app"}) == 1
    for: 6m
    labels:
      job: app
      severity: critical
      slo: http
      team: foo
- interval: 30s
  name: http
  partial_response_strategy: warn
  rules:
  - expr: sum(rate(http_requests_total{job="app",status=~"5.."}[5m])) / sum(rate(http_requests_total{job="app"}[5m]))
    labels:
      job: app
      slo: http
      team: foo
    record: http_requests:burnrate5m
  - expr: sum(rate(http_requests_total{job="app",status=~"5.."}[30m])) / sum(rate(http_requests_total{job="app"}[30m]))
    labels:
      job: app
      slo: http
      team: foo
    record: http_requests:burnrate30m
  - expr: sum(rate(http_requests_total{job="app",status=~"5.."}[1h])) / sum(rate(http_requests_total{job="app"}[1h]))
    labels:
      job: app
      slo: http
      team: foo
    record: http_requests:burnrate1h
  - expr: sum(rate(http_requests_total{job="app",status=~"5.."}[2h])) / sum(rate(http_requests_total{job="app"}[2h]))
    labels:
      job: app
      slo: http
      team: foo
    record: http_requests:burnrate2h
  - expr: sum(rate(http_requests_total{job="app",status=~"5.."}[6h])) / sum(rate(http_requests_total{job="app"}[6h]))
    labels:
      job: app
      slo: http
      team: foo
    record: http_requests:burnrate6h
  - expr: sum(rate(http_requests_total{job="app",status=~"5.."}[1d])) / sum(rate(http_requests_total{job="app"}[1d]))
    labels:
      job: app
      slo: http
      team: foo
    record: http_requests:burnrate1d
  - expr: sum(rate(http_requests_total{job="app",status=~"5.."}[4d])) / sum(rate(http_requests_total{job="app"}[4d]))
    labels:
      job: app
      slo: http
      team: foo
    record: http_requests:burnrate4d
  - alert: ErrorBudgetBurn
    annotations:
      description: foo
    expr: http_requests:burnrate5m{job="app",slo="http"} > (14 * (1-0.995)) and http_requests:burnrate1h{job="app",slo="http"}
      > (14 * (1-0.995))
    for: 2m0s
    labels:
      exhaustion: 2d
      job: app
      long: 1h
      severity: critical
      short: 5m
      slo: http
      team: foo
  - alert: ErrorBudgetBurn
    annotations:
      description: foo
    expr: http_requests:burnrate30m{job="app",slo="http"} > (7 * (1-0.995)) and http_requests:burnrate6h{job="app",slo="http"}
      > (7 * (1-0.995))
    for: 15m0s
    labels:
      exhaustion: 4d
      job: app
      long: 6h
      severity: critical
      short: 30m
      slo: http
      team: foo
  - alert: ErrorBudgetBurn
    annotations:
      description: foo
    expr: http_requests:burnrate2h{job="app",slo="http"} > (2 * (1-0.995)) and http_requests:burnrate1d{job="app",slo="http"}
      > (2 * (1-0.995))
    for: 1h0m0s
    labels:
      exhaustion: 2w
      job: app
      long: 1d
      severity: warning
      short: 2h
      slo: http
      team: foo
  - alert: ErrorBudgetBurn
    annotations:
      description: foo
    expr: http_requests:burnrate6h{job="app",slo="http"} > (1 * (1-0.995)) and http_requests:burnrate4d{job="app",slo="http"}
      > (1 * (1-0.995))
    for: 3h0m0s
    labels:
      exhaustion: 4w
      job: app
      long: 4d
      severity: warning
      short: 6h
      slo: http
      team: foo
`

	testcases := []struct {
		name          string
		configMapName string
		objective     pyrrav1alpha1.ServiceLevelObjective

		want *corev1.ConfigMap
		err  error
	}{
		{
			name: "NoInput",
			err:  fmt.Errorf("failed to get objective"),
		},
		{
			name:          "HTTP",
			configMapName: "http",
			objective:     httpSLO,
			want: &corev1.ConfigMap{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "ConfigMap",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "http",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: pyrrav1alpha1.GroupVersion.Version,
							Kind:       "ServiceLevelObjective",
							Name:       "http",
							UID:        "123",
							Controller: &trueBool,
						},
					},
					Labels: map[string]string{
						slo.PropagationLabelsPrefix + "team": "foo",
						"team":                               "bar",
					},
					Annotations: map[string]string{
						slo.PropagationLabelsPrefix + "description": "foo",
						"description": "bar",
					},
				},
				Data: map[string]string{
					"http.rules.yaml": rules,
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			configMap, err := makeConfigMap(tc.configMapName, tc.objective, false, false, "")

			if tc.err != nil {
				require.Error(t, err)
				require.ErrorAs(t, err, &tc.err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.want, configMap)
		})
	}
}

func monitoringDuration(d string) *monitoringv1.Duration {
	md := monitoringv1.Duration(d)
	return &md
}

func thresholdSLO() pyrrav1alpha1.ServiceLevelObjective {
	objective := httpSLO.DeepCopy()
	objective.Spec.ServiceLevelIndicator = pyrrav1alpha1.ServiceLevelIndicator{
		Threshold: &pyrrav1alpha1.ThresholdIndicator{
			Gauge: &pyrrav1alpha1.Query{Metric: `queue_depth{job="app"}`},
			Op:    "lte",
			Value: new(100.0),
		},
	}
	return *objective
}

func TestThresholdRuleOutputs(t *testing.T) {
	for _, genericRules := range []bool{false, true} {
		t.Run(fmt.Sprintf("generic=%t", genericRules), func(t *testing.T) {
			objective := thresholdSLO()
			rule, err := makePrometheusRule(objective, genericRules, false, "")
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(rule.Spec.Groups), 3)
			threshold := rule.Spec.Groups[0]
			require.Equal(t, "http-threshold", threshold.Name)
			require.Equal(t, monitoringDuration("1m"), threshold.Interval)
			require.Len(t, threshold.Rules, 1)
			require.Equal(t, "warn", threshold.PartialResponseStrategy)
			for _, group := range rule.Spec.Groups[1:] {
				require.NotEqual(t, threshold.Name, group.Name)
			}

			configMap, err := makeConfigMap("http", objective, genericRules, false, "")
			require.NoError(t, err)
			var spec monitoringv1.PrometheusRuleSpec
			require.NoError(t, yaml.UnmarshalStrict([]byte(configMap.Data["http.rules.yaml"]), &spec))
			require.Equal(t, rule.Spec, spec)

			objective.Spec.PerformanceOverAccuracy = true
			short, long, err := makeSplitPrometheusRules(objective, genericRules, false, "")
			require.NoError(t, err)
			require.Equal(t, threshold, short.Spec.Groups[0])
			for _, group := range long.Spec.Groups {
				require.NotEqual(t, threshold.Name, group.Name)
				for _, r := range group.Rules {
					require.NotEqual(t, threshold.Rules[0].Record, r.Record)
				}
			}
		})
	}
}

func TestMakeMimirRuleGroups(t *testing.T) {
	for _, objective := range []pyrrav1alpha1.ServiceLevelObjective{httpSLO, thresholdSLO()} {
		for _, genericRules := range []bool{false, true} {
			for _, writeAlerts := range []bool{false, true} {
				t.Run(fmt.Sprintf("threshold=%t/generic=%t/alerts=%t", objective.Spec.ServiceLevelIndicator.Threshold != nil, genericRules, writeAlerts), func(t *testing.T) {
					groups, err := makeMimirRuleGroups(objective, genericRules, writeAlerts, false, "")
					require.NoError(t, err)
					promRule, err := makePrometheusRule(objective, genericRules, false, "")
					require.NoError(t, err)
					promGroups := promRule.Spec.Groups
					if objective.Spec.ServiceLevelIndicator.Threshold != nil {
						require.Len(t, groups, 2)
						require.Equal(t, "http-threshold", groups[0].Name)
						require.Equal(t, model.Duration(time.Minute), groups[0].Interval)
						require.Equal(t, prometheusRulesToMimirRules(promGroups[0].Rules, writeAlerts), groups[0].Rules)
						promGroups = promGroups[1:]
					} else {
						require.Len(t, groups, 1)
					}
					combined := groups[len(groups)-1]
					require.Equal(t, "http", combined.Name)
					require.Equal(t, model.Duration(30*time.Second), combined.Interval)
					var expected []rulefmt.Rule
					for _, group := range promGroups {
						expected = append(expected, prometheusRulesToMimirRules(group.Rules, writeAlerts)...)
					}
					require.Equal(t, expected, combined.Rules)
				})
			}
		}
	}
}

func TestMimirThresholdLifecycle(t *testing.T) {
	for _, failDeletion := range []bool{false, true} {
		t.Run(fmt.Sprintf("failDeletion=%t", failDeletion), func(t *testing.T) {
			var mu sync.Mutex
			groups := map[string]rulefmt.RuleGroup{}
			deletes := 0
			failNextDelete := failDeletion
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if req.Header.Get(mimir.TenantHeaderName) != "test-tenant" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if req.Method == http.MethodDelete && req.URL.Path == "/prometheus/config/v1/rules/http/http-threshold" {
					if failNextDelete {
						failNextDelete = false
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					if _, ok := groups["http-threshold"]; !ok {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					delete(groups, "http-threshold")
					w.WriteHeader(http.StatusAccepted)
					return
				}
				if req.URL.Path != "/prometheus/config/v1/rules/http" {
					http.NotFound(w, req)
					return
				}
				switch req.Method {
				case http.MethodPost:
					var group rulefmt.RuleGroup
					if err := yamlv3.NewDecoder(req.Body).Decode(&group); err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					groups[group.Name] = group
				case http.MethodDelete:
					if len(groups) == 0 {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					clear(groups)
					deletes++
				default:
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				w.WriteHeader(http.StatusAccepted)
			}))
			defer server.Close()
			mimirClient, err := mimir.NewClient(mimir.Config{Address: server.URL, OrgID: "test-tenant"})
			require.NoError(t, err)
			scheme := runtime.NewScheme()
			require.NoError(t, pyrrav1alpha1.AddToScheme(scheme))
			objective := thresholdSLO()
			kubeClient := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&pyrrav1alpha1.ServiceLevelObjective{}).
				WithObjects(&objective).Build()
			r := ServiceLevelObjectiveReconciler{Client: kubeClient, MimirClient: mimirClient, Logger: log.NewNopLogger()}
			ctx := context.Background()
			key := client.ObjectKeyFromObject(&objective)
			req := ctrl.Request{NamespacedName: key}
			_, err = r.Reconcile(ctx, req)
			require.NoError(t, err)
			mu.Lock()
			require.Len(t, groups, 2)
			require.Equal(t, model.Duration(time.Minute), groups["http-threshold"].Interval)
			mu.Unlock()

			require.NoError(t, kubeClient.Get(ctx, key, &objective))
			objective.Spec.ServiceLevelIndicator = httpSLO.Spec.ServiceLevelIndicator
			require.NoError(t, kubeClient.Update(ctx, &objective))
			_, err = r.Reconcile(ctx, req)
			if failDeletion {
				require.Error(t, err)
				_, err = r.Reconcile(ctx, req)
			}
			require.NoError(t, err)
			mu.Lock()
			require.Len(t, groups, 1)
			require.Contains(t, groups, "http")
			previousDeletes := deletes
			mu.Unlock()
			_, err = r.Reconcile(ctx, req)
			require.NoError(t, err)
			mu.Lock()
			require.Equal(t, previousDeletes, deletes, "ordinary reconciles must not delete namespaces")
			mu.Unlock()

			// Switch back, then delete the SLO through its finalizer: both groups go.
			require.NoError(t, kubeClient.Get(ctx, key, &objective))
			objective.Spec.ServiceLevelIndicator = thresholdSLO().Spec.ServiceLevelIndicator
			require.NoError(t, kubeClient.Update(ctx, &objective))
			_, err = r.Reconcile(ctx, req)
			require.NoError(t, err)
			require.NoError(t, kubeClient.Get(ctx, key, &objective))
			require.NoError(t, kubeClient.Delete(ctx, &objective))
			_, err = r.Reconcile(ctx, req)
			require.NoError(t, err)
			mu.Lock()
			require.Empty(t, groups)
			require.Equal(t, 1, deletes)
			mu.Unlock()
			require.True(t, apierrors.IsNotFound(kubeClient.Get(ctx, key, &objective)))
		})
	}
}
