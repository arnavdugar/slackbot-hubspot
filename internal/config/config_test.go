package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const valid = `hubspot:
  access_token_env: TEST_HUBSPOT_TOKEN
  account_id: "123"
slack:
  bot_token_env: TEST_SLACK_TOKEN
  bot_user_id: UBOT
  signing_secret_env: TEST_SLACK_SECRET
  workspace_id: TTEAM
storage:
  driver: sqlite
  sqlite:
    path: /tmp/test.db
  postgres:
    dsn_env: NOT_SET_FOR_UNUSED_DRIVER
summary_template: "{{.Subject}} $100 ${TEST_DEPLOYMENT}"
`

func configFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func environment(t *testing.T) {
	t.Helper()
	t.Setenv("TEST_DEPLOYMENT", "example")
	t.Setenv("TEST_HUBSPOT_TOKEN", "test-hubspot-token")
	t.Setenv("TEST_SLACK_SECRET", "test-signing-secret")
	t.Setenv("TEST_SLACK_TOKEN", "test-slack-token")
}

func TestLoadDefaults(t *testing.T) {
	environment(t)
	path := configFile(t, valid)

	c, err := Load(path)

	if err != nil {
		t.Fatal(err)
	}
	if c.Worker.LeaseDuration != 2*time.Minute || c.Slack.SuccessReaction != "white_check_mark" {
		t.Fatalf("defaults = %+v, %+v", c.Worker, c.Slack)
	}
}

func TestLoadResolvesSigningSecret(t *testing.T) {
	environment(t)
	path := configFile(t, valid)

	c, err := Load(path)

	if err != nil || c.Slack.SigningSecret != "test-signing-secret" {
		t.Fatalf("secret resolution failed: %v", err)
	}
}

func TestLoadInterpolatesTemplateEnvironment(t *testing.T) {
	environment(t)
	path := configFile(t, valid)

	c, err := Load(path)

	if err != nil || c.SummaryTemplate != "{{.Subject}} $100 example" {
		t.Fatalf("template = %q, %v", c.SummaryTemplate, err)
	}
}

func TestLoadIgnoresUnselectedStorageDriver(t *testing.T) {
	environment(t)
	path := configFile(t, valid)

	c, err := Load(path)

	if err != nil {
		t.Fatal(err)
	}
	if c.Storage["postgres"].(map[string]any)["dsn_env"] != "NOT_SET_FOR_UNUSED_DRIVER" {
		t.Fatal("unselected driver was changed")
	}
}

func TestTrackingReactionConfiguration(t *testing.T) {
	environment(t)
	t.Setenv("TEST_TRACKING_REACTION", "pushpin")
	for _, tc := range []struct{ name, setting, want string }{
		{name: "default emoji", want: "eyes"},
		{name: "custom emoji", setting: "tracking_reaction: bookmark", want: "bookmark"},
		{name: "disabled", setting: `tracking_reaction: ""`, want: ""},
		{name: "environment variable", setting: "tracking_reaction_env: TEST_TRACKING_REACTION", want: "pushpin"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.Replace(valid, "slack:\n", "slack:\n  "+tc.setting+"\n", 1)

			c, err := Load(configFile(t, body))

			if err != nil {
				t.Fatal(err)
			}
			if c.Slack.TrackingReaction != tc.want {
				t.Fatalf("tracking reaction = %q, want %q", c.Slack.TrackingReaction, tc.want)
			}
		})
	}
}

func TestInvalidConfiguration(t *testing.T) {
	environment(t)
	for _, tc := range []struct{ name, body string }{
		{name: "unknown field", body: valid + "unknown_field: value\n"},
		{name: "multiple documents", body: valid + "---\nserver: {}\n"},
		{name: "replicated sqlite", body: valid + "worker:\n  replicas: 2\n"},
		{name: "invalid duration", body: valid + "worker:\n  lease_duration: yesterday\n"},
		{name: "invalid retry", body: valid + "worker:\n  backoff_max: 1ms\n"},
		{name: "unknown template variable", body: strings.Replace(valid, "{{.Subject}}", "{{.PrivateSecret}}", 1)},
		{name: "executable template", body: strings.Replace(valid, "{{.Subject}}", "{{call .Subject}}", 1)},
		{name: "missing secret", body: strings.Replace(valid, "TEST_SLACK_SECRET", "MISSING_SECRET_TEST", 1)},
		{name: "conflicting secret", body: strings.Replace(valid, "  signing_secret_env:", "  signing_secret: direct-secret\n  signing_secret_env:", 1)},
		{name: "long request deadline", body: valid + "server:\n  request_timeout: 3s\n"},
		{name: "invalid MIME", body: valid + "sync:\n  allowed_mime_types: ['image/*']\n"},
		{name: "invalid attachment limit", body: valid + "sync:\n  max_attachment_bytes: 9223372036854775807\n"},
		{name: "tracking matches success", body: strings.Replace(valid, "slack:\n", "slack:\n  tracking_reaction: white_check_mark\n", 1)},
		{name: "tracking matches failure", body: strings.Replace(valid, "slack:\n", "slack:\n  failure_reaction: eyes\n", 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Load(configFile(t, tc.body)); err == nil {
				t.Fatal("invalid config accepted")
			} else if strings.Contains(err.Error(), "direct-secret") || strings.Contains(err.Error(), "test-signing-secret") {
				t.Fatal("secret exposed by error")
			}
		})
	}
}

func TestLoadMountedSecret(t *testing.T) {
	environment(t)
	secret := filepath.Join(t.TempDir(), "signing-secret")
	if err := os.WriteFile(secret, []byte("mounted-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	path := configFile(t, strings.Replace(valid, "signing_secret_env: TEST_SLACK_SECRET", "signing_secret_file: "+secret, 1))

	c, err := Load(path)

	if err != nil || c.Slack.SigningSecret != "mounted-secret" {
		t.Fatalf("mounted secret = %q, %v", c.Slack.SigningSecret, err)
	}
}

func TestLoadStorageDoesNotRequireApplicationSecrets(t *testing.T) {
	path := configFile(t, "storage:\n  driver: sqlite\n  sqlite:\n    path: /tmp/test.db\nslack:\n  bot_token_env: MISSING_SECRET_TEST\n")

	storage, err := LoadStorage(path)

	if err != nil || storage["driver"] != "sqlite" {
		t.Fatalf("storage configuration = %+v, %v", storage, err)
	}
}

func TestEnvironmentScalarTypes(t *testing.T) {
	for _, tc := range []struct {
		name, variable, value, setting string
		get                            func(Config) any
		want                           any
	}{
		{"integer interpolation", "TEST_PORT", "8081", "server:\n  port: ${TEST_PORT}\n", func(c Config) any { return c.Server.Port }, 8081},
		{"integer environment setting", "TEST_CONCURRENCY", "2", "worker:\n  concurrency_env: TEST_CONCURRENCY\n", func(c Config) any { return c.Worker.Concurrency }, 2},
		{"boolean environment setting", "TEST_ATTACHMENTS", "false", "sync:\n  attachments_env: TEST_ATTACHMENTS\n", func(c Config) any { return c.Sync.Attachments }, false},
		{"list string interpolation", "TEST_MIME", "application/pdf", "sync:\n  allowed_mime_types: ['${TEST_MIME}']\n", func(c Config) any { return c.Sync.AllowedMIMETypes[0] }, "application/pdf"},
		{"numeric secret remains a string", "TEST_SLACK_SECRET", "012345", "", func(c Config) any { return c.Slack.SigningSecret }, "012345"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			environment(t)
			t.Setenv(tc.variable, tc.value)
			path := configFile(t, valid+tc.setting)

			c, err := Load(path)

			if err != nil {
				t.Fatal(err)
			}
			if got := tc.get(c); got != tc.want {
				t.Errorf("scalar = %v (%T), want %v (%T)", got, got, tc.want, tc.want)
			}
		})
	}
}
