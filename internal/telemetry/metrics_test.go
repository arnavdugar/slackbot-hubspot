package telemetry_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"slackhubspot/internal/domain"
	"slackhubspot/internal/httpapi"
	"slackhubspot/internal/telemetry"
)

type metricsStore struct {
	domain.Store
	counts func(context.Context) (domain.WorkCounts, error)
}

func (s metricsStore) Counts(ctx context.Context, _ time.Time) (domain.WorkCounts, error) {
	return s.counts(ctx)
}

func scrape(t *testing.T, handler http.Handler, accept string) map[string]*dto.MetricFamily {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	request.Header.Set("Accept", accept)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("scrape status %d: %s", response.Code, response.Body)
	}
	if !strings.HasPrefix(response.Header().Get("Content-Type"), strings.Split(accept, ";")[0]) {
		t.Fatalf("unexpected content type %q", response.Header().Get("Content-Type"))
	}
	families := make(map[string]*dto.MetricFamily)
	decoder := expfmt.NewDecoder(response.Body, expfmt.ResponseFormat(response.Header()))
	for {
		family := new(dto.MetricFamily)
		if err := decoder.Decode(family); err == io.EOF {
			return families
		} else if err != nil {
			t.Fatal(err)
		}
		families[family.GetName()] = family
	}
}

func TestMetricsThroughStrictAPI(t *testing.T) {
	store := metricsStore{counts: func(context.Context) (domain.WorkCounts, error) {
		return domain.WorkCounts{Leased: 2, Pending: 3}, nil
	}}
	metrics := telemetry.New(store)
	handler := httpapi.New(httpapi.Options{Metrics: metrics})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				metrics.Inc("slack_events_received")
				metrics.ObserveSync(2 * time.Second)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			if response.Code != http.StatusOK {
				t.Errorf("concurrent scrape status %d", response.Code)
			}
		})
	}
	wg.Wait()
	metrics.Inc("unbounded-user-input")
	for _, accept := range []string{
		"application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited",
		"text/plain; version=0.0.4",
	} {
		t.Run(accept, func(t *testing.T) {
			families := scrape(t, handler, accept)
			for name, want := range map[string]float64{
				"slackhubspot_leased_work":                 2,
				"slackhubspot_pending_work":                3,
				"slackhubspot_slack_events_received_total": 800,
			} {
				family := families[name]
				if len(family.GetMetric()) != 1 {
					t.Fatalf("missing metric %s", name)
				}
				metric := family.Metric[0]
				got := metric.GetGauge().GetValue() + metric.GetCounter().GetValue()
				if got != want || len(metric.Label) != 0 {
					t.Errorf("%s = %v; want %v without labels", name, metric, want)
				}
			}
			family := families["slackhubspot_sync_duration_seconds"]
			if len(family.GetMetric()) != 1 {
				t.Fatal("missing synchronization histogram")
			}
			histogram := family.Metric[0].GetHistogram()
			if histogram.GetSampleCount() != 800 || histogram.GetSampleSum() != 1600 || len(histogram.GetBucket()) == 0 {
				t.Fatalf("unexpected histogram %v", histogram)
			}
			if families["go_goroutines"] == nil || families["slackhubspot_unbounded-user-input_total"] != nil {
				t.Fatal("missing runtime metrics or accepted unbounded metric name")
			}
		})
	}
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	request.Header.Set("Accept", "application/openmetrics-text; version=1.0.0")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.HasPrefix(response.Header().Get("Content-Type"), "application/openmetrics-text;") || !strings.HasSuffix(response.Body.String(), "# EOF\n") {
		t.Fatal("OpenMetrics negotiation or response framing failed")
	}
	// Independent service instances must not share a global registry or counts.
	isolated := scrape(t, telemetry.New(store), "text/plain")
	if isolated["slackhubspot_slack_events_received_total"].Metric[0].GetCounter().GetValue() != 0 {
		t.Fatal("counter leaked across registries")
	}
}

func TestMetricsStorageFailureAndDeadline(t *testing.T) {
	for _, tc := range []struct {
		counts func(context.Context) (domain.WorkCounts, error)
		name   string
	}{
		{func(context.Context) (domain.WorkCounts, error) {
			return domain.WorkCounts{}, errors.New("private database connection details")
		}, "storage failure"},
		{func(ctx context.Context) (domain.WorkCounts, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("storage read has no request deadline")
			}
			<-ctx.Done()
			return domain.WorkCounts{}, ctx.Err()
		}, "request deadline"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics := telemetry.New(metricsStore{counts: tc.counts})
			handler := httpapi.New(httpapi.Options{Metrics: metrics, RequestTimeout: time.Millisecond})
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			if response.Code != http.StatusServiceUnavailable || response.Body.String() != "metrics storage unavailable\n" {
				t.Fatalf("unexpected failed scrape: %d %s", response.Code, response.Body)
			}
		})
	}
}
