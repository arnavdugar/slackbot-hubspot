// Package telemetry exposes bounded-cardinality Prometheus metrics. Identifiers
// belong in structured logs, never labels.
package telemetry

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"slackhubspot/internal/domain"
)

type Metrics struct {
	counts   map[string]prometheus.Counter
	duration prometheus.Histogram
	registry *prometheus.Registry
	store    domain.Store
}

func New(store domain.Store) *Metrics {
	m := &Metrics{
		counts: make(map[string]prometheus.Counter),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets:   []float64{0.1, 0.5, 1, 5, 15, 30, 60, 120, 300, 600},
			Help:      "Duration of thread synchronization attempts in seconds.",
			Name:      "sync_duration_seconds",
			Namespace: "slackhubspot",
		}),
		registry: prometheus.NewRegistry(),
		store:    store,
	}
	for _, metric := range []struct{ help, name string }{
		{"Attachment processing failures.", "attachments_failed"},
		{"Attachments skipped by policy or unsupported metadata.", "attachments_skipped"},
		{"Attachments uploaded to HubSpot.", "attachments_uploaded"},
		{"Previously recorded Slack events received again.", "duplicate_events"},
		{"HubSpot rate limit responses.", "hubspot_rate_limits"},
		{"Expired work leases recovered.", "lease_recovery"},
		{"Thread synchronization attempts with permanent failures.", "permanent_failures"},
		{"Thread synchronization retries scheduled.", "retries"},
		{"Slack event requests received.", "slack_events_received"},
		{"Slack event requests rejected.", "slack_events_rejected"},
		{"Slack rate limit responses.", "slack_rate_limits"},
		{"HubSpot transcript notes created.", "transcript_creates"},
		{"HubSpot transcript notes updated.", "transcript_updates"},
	} {
		counter := prometheus.NewCounter(prometheus.CounterOpts{
			Help: metric.help, Name: metric.name + "_total", Namespace: "slackhubspot",
		})
		m.registry.MustRegister(counter)
		m.counts[metric.name] = counter
	}
	m.registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), m.duration)
	return m
}

// Inc accepts a predefined metric name only, keeping labels and memory bounded.
func (m *Metrics) Inc(name string) {
	if m != nil {
		if counter, ok := m.counts[name]; ok {
			counter.Inc()
		}
	}
}

func (m *Metrics) ObserveSync(d time.Duration) {
	if m != nil {
		m.duration.Observe(d.Seconds())
	}
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	counts, err := m.store.Counts(r.Context(), time.Now())
	if err != nil {
		http.Error(w, "metrics storage unavailable", http.StatusServiceUnavailable)
		return
	}
	// Keep the database read within the request deadline and its snapshot local
	// to this scrape. Shared gauges could mix values from concurrent scrapes.
	snapshot := prometheus.NewRegistry()
	snapshot.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Help: "Work items with active leases.", Name: "leased_work", Namespace: "slackhubspot",
		}, func() float64 { return float64(counts.Leased) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Help: "Work items pending synchronization.", Name: "pending_work", Namespace: "slackhubspot",
		}, func() float64 { return float64(counts.Pending) }),
	)
	promhttp.HandlerFor(prometheus.Gatherers{m.registry, snapshot}, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}).ServeHTTP(w, r)
}
