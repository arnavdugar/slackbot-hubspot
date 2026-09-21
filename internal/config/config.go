// Package config loads mounted configuration without coupling callers to drivers.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"slackhubspot/internal/render"
)

type Config struct {
	HubSpot         HubSpot        `yaml:"hubspot"`
	Server          Server         `yaml:"server"`
	Slack           Slack          `yaml:"slack"`
	Storage         map[string]any `yaml:"storage"`
	SummaryTemplate string         `yaml:"summary_template"`
	Sync            Sync           `yaml:"sync"`
	Worker          Worker         `yaml:"worker"`
}

type HubSpot struct {
	AccessToken string `yaml:"access_token"`
	AccountID   string `yaml:"account_id"`
}

type Server struct {
	MaxBodyBytes      int64         `yaml:"max_body_bytes"`
	Port              int           `yaml:"port"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	RequestTimeout    time.Duration `yaml:"request_timeout"`
}

type Slack struct {
	BotToken        string `yaml:"bot_token"`
	BotUserID       string `yaml:"bot_user_id"`
	FailureReaction string `yaml:"failure_reaction"`
	HistoryToken    string `yaml:"history_token"`
	SigningSecret   string `yaml:"signing_secret"`
	SuccessReaction string `yaml:"success_reaction"`
	WorkspaceID     string `yaml:"workspace_id"`
}

type Sync struct {
	AllowedMIMETypes   []string `yaml:"allowed_mime_types"`
	Attachments        bool     `yaml:"attachments"`
	MaxAttachmentBytes int64    `yaml:"max_attachment_bytes"`
	TempDir            string   `yaml:"temp_dir"`
}

type Worker struct {
	BackoffBase   time.Duration `yaml:"backoff_base"`
	BackoffMax    time.Duration `yaml:"backoff_max"`
	CoalesceDelay time.Duration `yaml:"coalesce_delay"`
	Concurrency   int           `yaml:"concurrency"`
	GracePeriod   time.Duration `yaml:"grace_period"`
	LeaseDuration time.Duration `yaml:"lease_duration"`
	MaxAttempts   int           `yaml:"max_attempts"`
	PollInterval  time.Duration `yaml:"poll_interval"`
	Replicas      int           `yaml:"replicas"`
}

// LoadStorage resolves only the selected backend, allowing the migration binary
// to run without Slack or HubSpot credentials. Driver schemas are validated by
// the storage registry when opened.
func LoadStorage(path string) (map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read configuration: %w", err)
	}
	var raw map[string]any
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&raw); err != nil {
		return nil, errors.New("invalid configuration YAML")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("configuration must contain exactly one YAML document")
	}
	storage, ok := raw["storage"].(map[string]any)
	if !ok {
		return nil, errors.New("storage configuration is required")
	}
	if err := resolveStorage(storage); err != nil {
		return nil, err
	}
	return storage, nil
}

func Load(path string) (Config, error) {
	c := Config{
		Server: Server{MaxBodyBytes: 1 << 20, Port: 8080, ReadHeaderTimeout: 5 * time.Second, RequestTimeout: 2500 * time.Millisecond},
		Slack:  Slack{SuccessReaction: "white_check_mark"},
		Sync:   Sync{AllowedMIMETypes: []string{"application/pdf", "image/jpeg", "image/png", "text/plain"}, Attachments: true, MaxAttachmentBytes: 25_000_000, TempDir: os.TempDir()},
		Worker: Worker{BackoffBase: time.Second, BackoffMax: 5 * time.Minute, CoalesceDelay: time.Second, Concurrency: 4, GracePeriod: 30 * time.Second, LeaseDuration: 2 * time.Minute, MaxAttempts: 8, PollInterval: time.Second, Replicas: 1},
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return c, fmt.Errorf("read configuration: %w", err)
	}
	var raw map[string]any
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&raw); err != nil {
		return c, errors.New("invalid configuration YAML")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return c, errors.New("configuration must contain exactly one YAML document")
	}
	storage, ok := raw["storage"].(map[string]any)
	if !ok {
		return c, errors.New("storage configuration is required")
	}
	if err := resolveStorage(storage); err != nil {
		return c, err
	}
	delete(raw, "storage")
	if err := resolve(raw, "configuration"); err != nil {
		return c, err
	}
	// Environment variables are strings. Convert only values whose declared
	// application schema is numeric/boolean; secret and driver values retain
	// their exact spelling. Drivers own their own configuration conversion.
	schema := reflect.TypeFor[Config]()
	for i := 0; i < schema.NumField(); i++ {
		section := schema.Field(i)
		if section.Type.Kind() != reflect.Struct {
			continue
		}
		values, ok := raw[section.Tag.Get("yaml")].(map[string]any)
		if !ok {
			continue
		}
		for j := 0; j < section.Type.NumField(); j++ {
			field := section.Type.Field(j)
			key := field.Tag.Get("yaml")
			text, ok := values[key].(string)
			if !ok || field.Type == reflect.TypeFor[time.Duration]() {
				continue
			}
			var err error
			switch field.Type.Kind() {
			case reflect.Bool:
				values[key], err = strconv.ParseBool(text)
			case reflect.Int, reflect.Int64:
				values[key], err = strconv.ParseInt(text, 10, field.Type.Bits())
			}
			if err != nil {
				return c, fmt.Errorf("%s.%s has an invalid value", section.Tag.Get("yaml"), key)
			}
		}
	}
	raw["storage"] = storage
	data, err = yaml.Marshal(raw)
	if err != nil {
		return c, errors.New("invalid configuration")
	}
	decoder = yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&c); err != nil {
		return c, errors.New("configuration contains an unknown field or invalid value")
	}
	if c.Server.Port < 1 || c.Server.Port > 65535 || c.Server.MaxBodyBytes < 1024 || c.Server.MaxBodyBytes > 16<<20 {
		return c, errors.New("server port must be 1–65535 and max_body_bytes must be 1024–16777216")
	}
	if c.Server.ReadHeaderTimeout <= 0 || c.Server.RequestTimeout <= 0 || c.Server.RequestTimeout >= 3*time.Second {
		return c, errors.New("server timeouts must be positive and request_timeout must be below 3s")
	}
	if c.Slack.BotToken == "" || c.Slack.BotUserID == "" || c.Slack.SigningSecret == "" || c.Slack.WorkspaceID == "" || c.HubSpot.AccessToken == "" || c.HubSpot.AccountID == "" {
		return c, errors.New("Slack bot token, bot user ID, signing secret, workspace ID, HubSpot access token, and account ID are required")
	}
	if _, err := strconv.ParseUint(c.HubSpot.AccountID, 10, 64); err != nil {
		return c, errors.New("hubspot account_id must be numeric")
	}
	if c.Worker.BackoffBase <= 0 || c.Worker.BackoffMax < c.Worker.BackoffBase || c.Worker.CoalesceDelay < 0 || c.Worker.Concurrency < 1 || c.Worker.GracePeriod <= 0 || c.Worker.LeaseDuration < 3*time.Second || c.Worker.MaxAttempts < 1 || c.Worker.PollInterval <= 0 || c.Worker.Replicas < 1 {
		return c, errors.New("invalid worker settings: durations, concurrency, attempts, and replicas must be positive; lease_duration must be at least 3s; backoff_max must be at least backoff_base")
	}
	if c.Storage["driver"] == "sqlite" && c.Worker.Replicas != 1 {
		return c, errors.New("SQLite requires exactly one application replica")
	}
	if c.Sync.MaxAttachmentBytes <= 0 || c.Sync.MaxAttachmentBytes > 1<<30 || c.Sync.TempDir == "" {
		return c, errors.New("sync max_attachment_bytes must be 1–1073741824 and temp_dir must be set")
	}
	if c.Sync.Attachments && len(c.Sync.AllowedMIMETypes) == 0 {
		return c, errors.New("sync allowed_mime_types is required when attachments are enabled")
	}
	for i, allowed := range c.Sync.AllowedMIMETypes {
		mediaType, parameters, err := mime.ParseMediaType(allowed)
		if err != nil || !strings.Contains(mediaType, "/") || len(parameters) != 0 || strings.Contains(mediaType, "*") {
			return c, errors.New("sync allowed_mime_types must contain explicit MIME types")
		}
		c.Sync.AllowedMIMETypes[i] = mediaType
	}
	if strings.TrimSpace(c.SummaryTemplate) == "" {
		return c, errors.New("summary_template is required")
	}
	if _, err := render.CompileSummary(c.SummaryTemplate); err != nil {
		return c, fmt.Errorf("summary_template: %w", err)
	}
	return c, nil
}

func resolveStorage(storage map[string]any) error {
	driver, ok := storage["driver"].(string)
	if !ok {
		return errors.New("storage driver is required")
	}
	name := map[string]any{"driver": driver}
	if err := resolve(name, "storage"); err != nil {
		return err
	}
	driver = name["driver"].(string)
	if driver == "" {
		return errors.New("storage driver is required")
	}
	storage["driver"] = driver
	selected, ok := storage[driver].(map[string]any)
	if !ok {
		return errors.New("selected storage driver configuration is required")
	}
	return resolve(selected, "storage."+driver)
}

// resolve supports ${ENV} in strings and sibling name_env/name_file sources.
// Only field names appear in errors, never secret values or file contents.
var environmentVariable = regexp.MustCompile(`\$\{([^{}]+)\}`)

func resolve(values map[string]any, location string) error {
	for key, value := range values {
		if list, ok := value.([]any); ok {
			for i, item := range list {
				entry := map[string]any{"value": item}
				if err := resolve(entry, location+"."+key); err != nil {
					return err
				}
				list[i] = entry["value"]
			}
			continue
		}
		if nested, ok := value.(map[string]any); ok {
			if err := resolve(nested, location+"."+key); err != nil {
				return err
			}
			continue
		}
		if text, ok := value.(string); ok {
			missing := false
			text = environmentVariable.ReplaceAllStringFunc(text, func(reference string) string {
				v, ok := os.LookupEnv(reference[2 : len(reference)-1])
				missing = missing || !ok
				return v
			})
			if missing {
				return fmt.Errorf("%s.%s references an unset environment variable", location, key)
			}
			values[key] = text
		}
	}
	for key, value := range values {
		suffix := ""
		if strings.HasSuffix(key, "_env") {
			suffix = "_env"
		}
		if strings.HasSuffix(key, "_file") {
			suffix = "_file"
		}
		if suffix == "" {
			continue
		}
		base := strings.TrimSuffix(key, suffix)
		if _, exists := values[base]; exists {
			return fmt.Errorf("%s.%s has conflicting value sources", location, base)
		}
		other := "_env"
		if suffix == "_env" {
			other = "_file"
		}
		if _, exists := values[base+other]; exists {
			return fmt.Errorf("%s.%s has conflicting value sources", location, base)
		}
		source, ok := value.(string)
		if !ok || source == "" {
			return fmt.Errorf("%s.%s needs a nonempty source", location, key)
		}
		var resolved string
		if suffix == "_env" {
			resolved, ok = os.LookupEnv(source)
			if !ok || resolved == "" {
				return fmt.Errorf("%s.%s references an unset or empty environment variable", location, key)
			}
		} else {
			data, err := os.ReadFile(source)
			if err != nil {
				return fmt.Errorf("%s.%s cannot read secret file", location, key)
			}
			resolved = strings.TrimSpace(string(data))
			if resolved == "" {
				return fmt.Errorf("%s.%s references an empty secret file", location, key)
			}
		}
		delete(values, key)
		values[base] = resolved
	}
	return nil
}
