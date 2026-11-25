// Notifications for the manual changer ("the human is the robot arm").
//
// Channels (all optional, all best-effort — never fails a tape op):
//   - TAPEBERG_WEBHOOK_URL     generic webhook: POST {"text": "..."}
//                              (Slack incoming-webhook compatible)
//   - TAPEBERG_WEBHOOK_TOKEN   optional Authorization: Bearer <token>
//   - TAPEBERG_TELEGRAM_TOKEN  Telegram bot token
//   - TAPEBERG_TELEGRAM_CHAT   Telegram chat id to message
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"
)

type Notifier struct {
	WebhookURL    string
	WebhookToken  string
	TelegramToken string
	TelegramChat  string
}

func NewNotifierFromEnv() *Notifier {
	return &Notifier{
		WebhookURL:    os.Getenv("TAPEBERG_WEBHOOK_URL"),
		WebhookToken:  os.Getenv("TAPEBERG_WEBHOOK_TOKEN"),
		TelegramToken: os.Getenv("TAPEBERG_TELEGRAM_TOKEN"),
		TelegramChat:  os.Getenv("TAPEBERG_TELEGRAM_CHAT"),
	}
}

func (n *Notifier) Enabled() bool {
	return n != nil && (n.WebhookURL != "" || (n.TelegramToken != "" && n.TelegramChat != ""))
}

func (n *Notifier) Channels() string {
	var on []string
	if n == nil {
		return "none"
	}
	if n.WebhookURL != "" {
		on = append(on, "webhook")
	}
	if n.TelegramToken != "" && n.TelegramChat != "" {
		on = append(on, "telegram")
	}
	if len(on) == 0 {
		return "none"
	}
	return fmt.Sprintf("%v", on)
}

// Notify delivers text on every configured channel; errors are returned
// but callers treat delivery as best-effort.
func (n *Notifier) Notify(text string) error {
	if !n.Enabled() {
		return nil
	}
	client := &http.Client{Timeout: 10 * time.Second}
	var firstErr error
	if n.WebhookURL != "" {
		body, _ := json.Marshal(map[string]string{"text": text})
		req, err := http.NewRequest("POST", n.WebhookURL, bytes.NewReader(body))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
			if n.WebhookToken != "" {
				req.Header.Set("Authorization", "Bearer "+n.WebhookToken)
			}
			if _, err := client.Do(req); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	if n.TelegramToken != "" && n.TelegramChat != "" {
		api := "https://api.telegram.org/bot" + n.TelegramToken + "/sendMessage"
		form := url.Values{"chat_id": {n.TelegramChat}, "text": {text}}.Encode()
		req, err := http.NewRequest("POST", api, bytes.NewReader([]byte(form)))
		if err == nil {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			if _, err := client.Do(req); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func queryEscape(s string) string { return url.QueryEscape(s) }
