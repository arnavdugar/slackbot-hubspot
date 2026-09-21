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

func TestLoadDefaultsAndSelectedDriver(t *testing.T) {
	environment(t)
	c, err := Load(configFile(t, valid))
	if err != nil {
		t.Fatal(err)
	}
	if c.Slack.SigningSecret != "test-signing-secret" || c.SummaryTemplate != "{{.Subject}} $100 example" || c.Worker.LeaseDuration != 2*time.Minute {
		t.Fatalf("resolved configuration incorrect: secret resolved=%v", c.Slack.SigningSecret != "")
	}
	if c.Storage["postgres"].(map[string]any)["dsn_env"] != "NOT_SET_FOR_UNUSED_DRIVER" {
		t.Fatal("unselected driver was changed")
	}
}

func TestInvalidConfiguration(t *testing.T) {
	environment(t)
	for _, tc := range []struct{ name, body string }{
		{"unknown field", valid + "unknown_field: value\n"},
		{"multiple documents", valid + "---\nserver: {}\n"},
		{"replicated sqlite", valid + "worker:\n  replicas: 2\n"},
		{"invalid duration", valid + "worker:\n  lease_duration: yesterday\n"},
		{"invalid retry", valid + "worker:\n  backoff_max: 1ms\n"},
		{"unknown template variable", strings.Replace(valid, "{{.Subject}}", "{{.PrivateSecret}}", 1)},
		{"executable template", strings.Replace(valid, "{{.Subject}}", "{{call .Subject}}", 1)},
		{"missing secret", strings.Replace(valid, "TEST_SLACK_SECRET", "MISSING_SECRET_TEST", 1)},
		{"conflicting secret", strings.Replace(valid, "  signing_secret_env:", "  signing_secret: direct-secret\n  signing_secret_env:", 1)},
		{"long request deadline", valid + "server:\n  request_timeout: 3s\n"},
		{"invalid MIME", valid + "sync:\n  allowed_mime_types: ['image/*']\n"},
		{"invalid attachment limit", valid + "sync:\n  max_attachment_bytes: 9223372036854775807\n"},
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

func TestMountedSecretAndMigrationConfig(t *testing.T) {
	environment(t)
	secret := filepath.Join(t.TempDir(), "signing-secret")
	if err := os.WriteFile(secret, []byte("mounted-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(configFile(t, strings.Replace(valid, "signing_secret_env: TEST_SLACK_SECRET", "signing_secret_file: "+secret, 1)))
	if err != nil || c.Slack.SigningSecret != "mounted-secret" {
		t.Fatalf("mounted secret failed: %v", err)
	}
	storage, err := LoadStorage(configFile(t, "storage:\n  driver: sqlite\n  sqlite:\n    path: /tmp/test.db\nslack:\n  bot_token_env: MISSING_SECRET_TEST\n"))
	if err != nil || storage["driver"] != "sqlite" {
		t.Fatalf("migration incorrectly required application secrets: %v", err)
	}
}

func TestEnvironmentScalarTypes(t *testing.T) {
	environment(t)
	t.Setenv("TEST_ATTACHMENTS", "false")
	t.Setenv("TEST_CONCURRENCY", "2")
	t.Setenv("TEST_MIME", "application/pdf")
	t.Setenv("TEST_PORT", "8081")
	t.Setenv("TEST_SLACK_SECRET", "012345")
	c, err := Load(configFile(t, valid+"server:\n  port: ${TEST_PORT}\nworker:\n  concurrency_env: TEST_CONCURRENCY\nsync:\n  allowed_mime_types: ['${TEST_MIME}']\n  attachments_env: TEST_ATTACHMENTS\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.Port != 8081 || c.Worker.Concurrency != 2 || c.Sync.Attachments || c.Sync.AllowedMIMETypes[0] != "application/pdf" || c.Slack.SigningSecret != "012345" {
		t.Fatal("environment scalar types were not preserved correctly")
	}
}
