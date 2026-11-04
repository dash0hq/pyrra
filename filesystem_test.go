package main

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"github.com/pyrra-dev/pyrra/slo"
)

func TestReloadEndpoint(t *testing.T) {
	promClient, err := api.NewClient(api.Config{Address: "http://localhost:9090"})
	require.NoError(t, err)

	// Without an override the default Prometheus /-/reload endpoint is used.
	require.Equal(t, "http://localhost:9090/-/reload", reloadEndpoint(promClient, nil).String())

	// An empty override (kong's default for an unset *url.URL flag) is ignored.
	require.Equal(t, "http://localhost:9090/-/reload", reloadEndpoint(promClient, &url.URL{}).String())

	// A configured override takes precedence over the Prometheus URL.
	override := &url.URL{Scheme: "http", Host: "localhost:17902", Path: "/rule/-/reload"}
	require.Equal(t, "http://localhost:17902/rule/-/reload", reloadEndpoint(promClient, override).String())
}

func TestMatchObjectives(t *testing.T) {
	obj1 := slo.Objective{Labels: labels.FromStrings("foo", "bar")}
	obj2 := slo.Objective{Labels: labels.FromStrings("foo", "bar", "ying", "yang")}
	obj3 := slo.Objective{Labels: labels.FromStrings("foo", "bar", "yes", "no")}
	obj4 := slo.Objective{Labels: labels.FromStrings("foo", "baz")}

	objectives := Objectives{objectives: map[string]slo.Objective{}}
	objectives.Set(obj1)
	objectives.Set(obj2)
	objectives.Set(obj3)
	objectives.Set(obj4)

	matches := objectives.Match([]*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "foo", "foo"),
	})
	require.Nil(t, matches)

	matches = objectives.Match([]*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
	})
	require.Len(t, matches, 3)
	require.Contains(t, matches, obj1)
	require.Contains(t, matches, obj2)
	require.Contains(t, matches, obj3)

	matches = objectives.Match([]*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "foo", "baz"),
	})
	require.Len(t, matches, 1)
	require.Contains(t, matches, obj4)

	matches = objectives.Match([]*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
		labels.MustNewMatcher(labels.MatchEqual, "ying", "yang"),
	})
	require.Len(t, matches, 1)
	require.Contains(t, matches, obj2)

	matches = objectives.Match([]*labels.Matcher{
		labels.MustNewMatcher(labels.MatchRegexp, "foo", "ba."),
	})
	require.Len(t, matches, 4)
	require.Contains(t, matches, obj1)
	require.Contains(t, matches, obj2)
	require.Contains(t, matches, obj3)
	require.Contains(t, matches, obj4)
}

func TestWriteThresholdRuleFiles(t *testing.T) {
	for _, threshold := range []bool{false, true} {
		for _, performance := range []bool{false, true} {
			for _, operator := range []bool{false, true} {
				for _, generic := range []bool{false, true} {
					t.Run(fmt.Sprintf("threshold=%t/performance=%t/operator=%t/generic=%t", threshold, performance, operator, generic), func(t *testing.T) {
						indicator := `bool_gauge:
      metric: queue_ready{job="app"}`
						if threshold {
							indicator = `threshold:
      gauge:
        metric: queue_depth{job="app"}
      op: lte
      value: 100`
						}
						input := fmt.Sprintf(`apiVersion: pyrra.dev/v1alpha1
kind: ServiceLevelObjective
metadata:
  name: queue
  namespace: default
spec:
  target: "99"
  window: 28d
  performanceOverAccuracy: %t
  indicator:
    %s
`, performance, indicator)
						file := filepath.Join(t.TempDir(), "queue.yaml")
						require.NoError(t, os.WriteFile(file, []byte(input), 0o600))
						output := t.TempDir()
						require.NoError(t, writeRuleFile(log.NewNopLogger(), file, output, generic, operator, false, ""))
						names := []string{"queue.yaml"}
						if performance {
							names = []string{"queue-short.yaml", "queue-long.yaml"}
						}
						files, err := os.ReadDir(output)
						require.NoError(t, err)
						require.Len(t, files, len(names))
						for _, name := range names {
							data, err := os.ReadFile(filepath.Join(output, name))
							require.NoError(t, err)
							var spec monitoringv1.PrometheusRuleSpec
							if operator {
								var rule monitoringv1.PrometheusRule
								require.NoError(t, yaml.UnmarshalStrict(data, &rule))
								spec = rule.Spec
							} else {
								require.NoError(t, yaml.UnmarshalStrict(data, &spec))
							}
							require.NotEmpty(t, spec.Groups)
							if threshold && name != "queue-long.yaml" {
								require.Equal(t, "queue-threshold", spec.Groups[0].Name)
								require.NotNil(t, spec.Groups[0].Interval)
								require.Equal(t, monitoringv1.Duration("1m"), *spec.Groups[0].Interval)
								require.Len(t, spec.Groups[0].Rules, 1)
								spec.Groups = spec.Groups[1:]
							}
							for _, group := range spec.Groups {
								require.NotEqual(t, "queue-threshold", group.Name)
							}
							if threshold && name == "queue-long.yaml" {
								for _, group := range spec.Groups {
									for _, rule := range group.Rules {
										require.NotEqual(t, "pyrra_threshold", rule.Record)
									}
								}
							}
						}
					})
				}
			}
		}
	}
}
