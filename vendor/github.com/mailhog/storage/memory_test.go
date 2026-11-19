package storage

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/mailhog/data"
)

func testMessage(id, body string) *data.Message {
	return &data.Message{
		ID: data.MessageID(id),
		From: &data.Path{
			Mailbox: "sender",
			Domain:  "example.com",
		},
		To: []*data.Path{{
			Mailbox: "recipient",
			Domain:  "example.com",
		}},
		Content: &data.Content{
			Headers: map[string][]string{
				"From":    {"sender@example.com"},
				"To":      {"recipient@example.com"},
				"Subject": {"test " + id},
			},
			Body: body,
		},
		Raw: &data.SMTPMessage{
			From: "sender@example.com",
			To:   []string{"recipient@example.com"},
			Data: "Subject: test " + id + "\r\n\r\n" + body,
		},
	}
}

func messageIDs(messages *data.Messages) []string {
	ids := make([]string, 0, len(*messages))
	for _, message := range *messages {
		ids = append(ids, string(message.ID))
	}
	return ids
}

func assertIDs(t *testing.T, messages *data.Messages, want ...string) {
	t.Helper()

	got := messageIDs(messages)
	if len(got) != len(want) {
		t.Fatalf("got %d messages (%v), want %d (%v)", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("message %d is %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
}

func TestListUsesArrivalOrderAndPagination(t *testing.T) {
	store := CreateInMemoryWithLimits(10, 0)
	for i := 0; i < 5; i++ {
		if _, err := store.Store(testMessage(fmt.Sprintf("m%d", i), "body")); err != nil {
			t.Fatal(err)
		}
	}

	messages, err := store.List(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, messages, "m4", "m3")

	messages, err = store.List(2, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, messages, "m2", "m1")

	messages, err = store.List(4, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, messages, "m0")

	messages, err = store.List(5, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, messages)
}

func TestStoreEvictsOldestByMessageCount(t *testing.T) {
	store := CreateInMemoryWithLimits(3, 0)
	for i := 0; i < 5; i++ {
		if _, err := store.Store(testMessage(fmt.Sprintf("m%d", i), "body")); err != nil {
			t.Fatal(err)
		}
	}

	if got := store.Count(); got != 3 {
		t.Fatalf("Count() = %d, want 3", got)
	}

	messages, err := store.List(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, messages, "m4", "m3", "m2")

	for _, id := range []string{"m0", "m1"} {
		message, err := store.Load(id)
		if err != nil {
			t.Fatal(err)
		}
		if message != nil {
			t.Fatalf("evicted message %s is still loaded", id)
		}
	}
}

func TestDefaultLimitBoundsLargeBurst(t *testing.T) {
	store := CreateInMemory()
	for i := 0; i < 50000; i++ {
		if _, err := store.Store(testMessage(fmt.Sprintf("m%05d", i), "body")); err != nil {
			t.Fatal(err)
		}
	}

	if got := store.Count(); got != DefaultMaxMessages {
		t.Fatalf("Count() = %d, want %d", got, DefaultMaxMessages)
	}

	messages, err := store.List(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, messages, "m49999", "m49998")
}

func TestStoreEvictsOldestByMemoryLimit(t *testing.T) {
	probe := CreateInMemoryWithLimits(0, 1<<30)
	if _, err := probe.Store(testMessage("probe", strings.Repeat("x", 1024))); err != nil {
		t.Fatal(err)
	}

	limit := probe.Bytes()*2 + probe.Bytes()/2
	store := CreateInMemoryWithLimits(0, limit)
	for i := 0; i < 3; i++ {
		if _, err := store.Store(testMessage(fmt.Sprintf("m%d", i), strings.Repeat("x", 1024))); err != nil {
			t.Fatal(err)
		}
	}

	if got := store.Count(); got != 2 {
		t.Fatalf("Count() = %d, want 2", got)
	}
	if got := store.Bytes(); got > limit {
		t.Fatalf("Bytes() = %d, limit %d", got, limit)
	}

	messages, err := store.List(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, messages, "m2", "m1")
}

func TestSearchUsesNewestFirstPagination(t *testing.T) {
	store := CreateInMemoryWithLimits(10, 0)
	bodies := []string{"plain", "needle one", "needle two", "plain"}
	for i, body := range bodies {
		if _, err := store.Store(testMessage(fmt.Sprintf("m%d", i), body)); err != nil {
			t.Fatal(err)
		}
	}

	messages, total, err := store.Search("containing", "NEEDLE", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Fatalf("Search total = %d, want 2", total)
	}
	assertIDs(t, messages, "m2", "m1")

	messages, total, err = store.Search("containing", "needle", 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Fatalf("Search total = %d, want 2", total)
	}
	assertIDs(t, messages, "m1")
}

func TestConcurrentWritesRemainBoundedAndOrderedPerProducer(t *testing.T) {
	const (
		producers     = 8
		perProducer   = 250
		retainedCount = 1000
	)

	store := CreateInMemoryWithLimits(retainedCount, 0)
	var wg sync.WaitGroup
	for producer := 0; producer < producers; producer++ {
		wg.Add(1)
		go func(producer int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				id := fmt.Sprintf("%d-%04d", producer, i)
				if _, err := store.Store(testMessage(id, "body")); err != nil {
					t.Errorf("Store(%s): %v", id, err)
					return
				}
			}
		}(producer)
	}
	wg.Wait()

	if got := store.Count(); got != retainedCount {
		t.Fatalf("Count() = %d, want %d", got, retainedCount)
	}

	messages, err := store.List(0, retainedCount+1)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(*messages); got != retainedCount {
		t.Fatalf("List returned %d messages, want %d", got, retainedCount)
	}

	seen := make(map[string]bool, retainedCount)
	lastPerProducer := make([]int, producers)
	for i := range lastPerProducer {
		lastPerProducer[i] = perProducer
	}
	for _, message := range *messages {
		id := string(message.ID)
		if seen[id] {
			t.Fatalf("duplicate message %s", id)
		}
		seen[id] = true

		var producer, sequence int
		if _, err := fmt.Sscanf(id, "%d-%d", &producer, &sequence); err != nil {
			t.Fatalf("invalid message ID %q: %v", id, err)
		}
		if sequence >= lastPerProducer[producer] {
			t.Fatalf("producer %d order regressed: %d after %d", producer, sequence, lastPerProducer[producer])
		}
		lastPerProducer[producer] = sequence
	}
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	store := CreateInMemoryWithLimits(256, 0)

	var wg sync.WaitGroup
	for producer := 0; producer < 4; producer++ {
		wg.Add(1)
		go func(producer int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				id := fmt.Sprintf("%d-%04d", producer, i)
				if _, err := store.Store(testMessage(id, "needle")); err != nil {
					t.Errorf("Store(%s): %v", id, err)
					return
				}
			}
		}(producer)
	}

	for reader := 0; reader < 4; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if _, err := store.List(0, 50); err != nil {
					t.Errorf("List: %v", err)
					return
				}
				if _, _, err := store.Search("containing", "needle", 0, 20); err != nil {
					t.Errorf("Search: %v", err)
					return
				}
				store.Count()
				store.Bytes()
				store.Load("0-0000")
			}
		}()
	}

	wg.Wait()
}
