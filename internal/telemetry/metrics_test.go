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
		t.Fatalf("content type = %q", response.Header().Get("Content-Type"))
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

func metricFixture() (*telemetry.Metrics, http.Handler) {
	metrics := telemetry.New(metricsStore{counts: func(context.Context) (domain.WorkCounts, error) { return domain.WorkCounts{Leased: 2, Pending: 3}, nil }})
	return metrics, httpapi.New(httpapi.Options{Metrics: metrics})
}

func TestMetricsNegotiatesFormats(t *testing.T) {
	for _, accept := range []string{
		"application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited",
		"text/plain; version=0.0.4",
		"application/openmetrics-text; version=1.0.0",
	} {
		t.Run(accept, func(t *testing.T) {
			metrics, handler := metricFixture()
			metrics.Inc("slack_events_received")
			request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			request.Header.Set("Accept", accept)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != http.StatusOK || !strings.HasPrefix(response.Header().Get("Content-Type"), strings.Split(accept, ";")[0]) {
				t.Fatalf("response = %d, %q", response.Code, response.Header().Get("Content-Type"))
			}
			if strings.HasPrefix(accept, "application/openmetrics-text") {
				if !strings.HasSuffix(response.Body.String(), "# EOF\n") {
					t.Error("missing OpenMetrics EOF")
				}
				return
			}
			decoder := expfmt.NewDecoder(response.Body, expfmt.ResponseFormat(response.Header()))
			found := false
			for {
				family := new(dto.MetricFamily)
				err := decoder.Decode(family)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("undecodable negotiated format: %v", err)
				}
				if family.GetName() == "slackhubspot_slack_events_received_total" {
					found = true
					if len(family.GetMetric()) != 1 || family.Metric[0].GetCounter().GetValue() != 1 {
						t.Errorf("negotiated format lost counter value: %v", family)
					}
				}
			}
			if !found {
				t.Error("negotiated format omitted metrics")
			}
		})
	}
}

func TestMetricsExposesWorkCounts(t *testing.T) {
	_, handler := metricFixture()

	families := scrape(t, handler, "text/plain")

	for name, want := range map[string]float64{"slackhubspot_leased_work": 2, "slackhubspot_pending_work": 3} {
		family := families[name]
		if len(family.GetMetric()) != 1 {
			t.Fatalf("missing metric %s", name)
		}
		metric := family.Metric[0]
		if metric.GetGauge().GetValue() != want || len(metric.Label) != 0 {
			t.Errorf("%s = %v, want %v without labels", name, metric, want)
		}
	}
}

func TestConcurrentMetricUpdatesAndScrapes(t *testing.T) {
	metrics, handler := metricFixture()
	var wg sync.WaitGroup

	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				metrics.Inc("slack_events_received")
				metrics.ObserveSync(2 * time.Second)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			if response.Code != http.StatusOK {
				t.Errorf("concurrent scrape status %d", response.Code)
			}
		}()
	}
	wg.Wait()
	families := scrape(t, handler, "text/plain")

	counter := families["slackhubspot_slack_events_received_total"]
	histogram := families["slackhubspot_sync_duration_seconds"]
	if len(counter.GetMetric()) != 1 || len(histogram.GetMetric()) != 1 {
		t.Fatal("missing counter or histogram")
	}
	if got := counter.Metric[0].GetCounter().GetValue(); got != 800 {
		t.Errorf("counter = %v, want 800", got)
	}
	h := histogram.Metric[0].GetHistogram()
	if h.GetSampleCount() != 800 || h.GetSampleSum() != 1600 || len(h.GetBucket()) == 0 {
		t.Errorf("histogram = %v", h)
	}
}

func TestMetricsRejectsUnboundedNames(t *testing.T) {
	metrics, handler := metricFixture()

	metrics.Inc("unbounded-user-input")
	families := scrape(t, handler, "text/plain")

	if families["slackhubspot_unbounded-user-input_total"] != nil {
		t.Fatal("unbounded metric name accepted")
	}
}

func TestMetricsExposesRuntimeCollectors(t *testing.T) {
	_, handler := metricFixture()

	families := scrape(t, handler, "text/plain")

	if families["go_goroutines"] == nil {
		t.Fatal("runtime metrics missing")
	}
}

func TestMetricRegistriesAreIndependent(t *testing.T) {
	first, _ := metricFixture()
	first.Inc("slack_events_received")

	_, second := metricFixture()
	families := scrape(t, second, "text/plain")

	counter := families["slackhubspot_slack_events_received_total"]
	if len(counter.GetMetric()) != 1 {
		t.Fatal("counter missing")
	}
	if got := counter.Metric[0].GetCounter().GetValue(); got != 0 {
		t.Errorf("counter leaked across registries: %v", got)
	}
}

func TestMetricsStorageFailureIsSanitized(t *testing.T) {
	metrics := telemetry.New(metricsStore{counts: func(context.Context) (domain.WorkCounts, error) {
		return domain.WorkCounts{}, errors.New("private database connection details")
	}})
	handler := httpapi.New(httpapi.Options{Metrics: metrics})
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if response.Code != http.StatusServiceUnavailable || response.Body.String() != "metrics storage unavailable\n" {
		t.Fatalf("failed scrape = %d %s", response.Code, response.Body)
	}
}

func TestMetricsStorageReadHasDeadline(t *testing.T) {
	var deadlinePresent bool
	var readError error
	metrics := telemetry.New(metricsStore{counts: func(ctx context.Context) (domain.WorkCounts, error) {
		_, deadlinePresent = ctx.Deadline()
		if !deadlinePresent {
			return domain.WorkCounts{}, errors.New("missing deadline")
		}
		<-ctx.Done()
		readError = ctx.Err()
		return domain.WorkCounts{}, readError
	}})
	handler := httpapi.New(httpapi.Options{Metrics: metrics, RequestTimeout: time.Millisecond})
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if !deadlinePresent || !errors.Is(readError, context.DeadlineExceeded) {
		t.Fatalf("deadline=%v, error=%v", deadlinePresent, readError)
	}
	if response.Code != http.StatusServiceUnavailable || response.Body.String() != "metrics storage unavailable\n" {
		t.Fatalf("failed scrape = %d %s", response.Code, response.Body)
	}
}
