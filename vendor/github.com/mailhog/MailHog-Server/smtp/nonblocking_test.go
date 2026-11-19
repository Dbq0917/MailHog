package smtp

import (
	"testing"
	"time"

	"github.com/mailhog/data"
	mhsmtp "github.com/mailhog/smtp"
	"github.com/mailhog/storage"
)

func TestAcceptMessageDoesNotBlockOnSlowUINotification(t *testing.T) {
	store := storage.CreateInMemoryWithLimits(10, 0)
	session := &Session{
		proto:       &mhsmtp.Protocol{Hostname: "test.example"},
		storage:     store,
		messageChan: make(chan *data.Message),
	}

	done := make(chan error, 1)
	go func() {
		_, err := session.acceptMessage(&data.SMTPMessage{
			From: "sender@example.com",
			To:   []string{"recipient@example.com"},
			Data: "Subject: test\r\n\r\nbody",
		})
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("acceptMessage blocked on an unconsumed UI notification channel")
	}

	if got := store.Count(); got != 1 {
		t.Fatalf("Count() = %d, want 1", got)
	}
}
