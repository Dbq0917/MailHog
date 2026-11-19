package storage

import (
	"errors"
	"strings"
	"sync"

	"github.com/mailhog/data"
)

const (
	// DefaultMaxMessages is the default number of messages retained in memory.
	DefaultMaxMessages = 10000

	// DefaultMaxMemoryBytes is the default estimated payload retained in memory.
	DefaultMaxMemoryBytes int64 = 256 * 1024 * 1024
)

type messageEntry struct {
	message *data.Message
	size    int64
	prev    *messageEntry
	next    *messageEntry
}

// InMemory is a bounded, concurrency-safe in memory storage backend.
//
// Messages are retained in arrival order. When either limit is exceeded the
// oldest messages are evicted first, so readers always see a contiguous suffix
// of the message stream.
type InMemory struct {
	mu sync.RWMutex

	oldest *messageEntry
	newest *messageEntry
	byID   map[string]*messageEntry
	count  int
	bytes  int64

	maxMessages int
	maxBytes    int64
}

// CreateInMemory creates a new in memory storage backend with default limits.
func CreateInMemory() *InMemory {
	return CreateInMemoryWithLimits(DefaultMaxMessages, DefaultMaxMemoryBytes)
}

// CreateInMemoryWithLimits creates a new in memory storage backend. A
// non-positive limit disables that particular limit.
func CreateInMemoryWithLimits(maxMessages int, maxBytes int64) *InMemory {
	return &InMemory{
		byID:        make(map[string]*messageEntry),
		maxMessages: maxMessages,
		maxBytes:    maxBytes,
	}
}

// Store stores a message and returns its storage ID.
func (memory *InMemory) Store(m *data.Message) (string, error) {
	if m == nil {
		return "", errors.New("message is nil")
	}

	memory.mu.Lock()
	defer memory.mu.Unlock()

	if memory.byID == nil {
		memory.byID = make(map[string]*messageEntry)
	}

	id := string(m.ID)
	if old, ok := memory.byID[id]; ok {
		memory.remove(old)
	}

	entry := &messageEntry{
		message: m,
		size:    estimateMessageSize(m),
		prev:    memory.newest,
	}
	if memory.newest == nil {
		memory.oldest = entry
	} else {
		memory.newest.next = entry
	}
	memory.newest = entry
	memory.byID[id] = entry
	memory.count++
	memory.bytes += entry.size

	for memory.limitExceeded() && memory.oldest != nil {
		memory.remove(memory.oldest)
	}

	return id, nil
}

func (memory *InMemory) limitExceeded() bool {
	if memory.maxMessages > 0 && memory.count > memory.maxMessages {
		return true
	}
	return memory.maxBytes > 0 && memory.bytes > memory.maxBytes
}

func (memory *InMemory) remove(entry *messageEntry) {
	if entry == nil {
		return
	}

	if entry.prev == nil {
		memory.oldest = entry.next
	} else {
		entry.prev.next = entry.next
	}
	if entry.next == nil {
		memory.newest = entry.prev
	} else {
		entry.next.prev = entry.prev
	}

	id := string(entry.message.ID)
	if memory.byID[id] == entry {
		delete(memory.byID, id)
	}
	memory.count--
	memory.bytes -= entry.size
	entry.prev = nil
	entry.next = nil
}

// Count returns the number of stored messages.
func (memory *InMemory) Count() int {
	memory.mu.RLock()
	defer memory.mu.RUnlock()
	return memory.count
}

// Bytes returns the estimated number of payload bytes retained in memory.
func (memory *InMemory) Bytes() int64 {
	memory.mu.RLock()
	defer memory.mu.RUnlock()
	return memory.bytes
}

// Search finds messages matching the query.
func (memory *InMemory) Search(kind, query string, start, limit int) (*data.Messages, int, error) {
	query = strings.ToLower(query)

	memory.mu.RLock()
	snapshot := make([]*data.Message, 0, memory.count)
	for entry := memory.oldest; entry != nil; entry = entry.next {
		snapshot = append(snapshot, entry.message)
	}
	memory.mu.RUnlock()

	matched := 0
	for i := len(snapshot) - 1; i >= 0; i-- {
		if messageMatches(snapshot[i], kind, query) {
			matched++
		}
	}

	messages := make([]data.Message, 0)
	if start < 0 {
		start = 0
	}
	if limit < 0 {
		limit = 0
	}
	if limit == 0 {
		msgs := data.Messages(messages)
		return &msgs, matched, nil
	}

	skipped := 0
	for i := len(snapshot) - 1; i >= 0 && len(messages) < limit; i-- {
		if !messageMatches(snapshot[i], kind, query) {
			continue
		}
		if skipped < start {
			skipped++
			continue
		}
		messages = append(messages, *snapshot[i])
	}

	msgs := data.Messages(messages)
	return &msgs, matched, nil
}

func messageMatches(m *data.Message, kind, query string) bool {
	if m == nil || m.Content == nil {
		return false
	}

	switch kind {
	case "to":
		for _, to := range m.To {
			if to == nil {
				continue
			}
			if strings.Contains(strings.ToLower(to.Mailbox+"@"+to.Domain), query) {
				return true
			}
		}
		if hdr, ok := m.Content.Headers["To"]; ok {
			for _, to := range hdr {
				if strings.Contains(strings.ToLower(to), query) {
					return true
				}
			}
		}
	case "from":
		if m.From != nil && strings.Contains(strings.ToLower(m.From.Mailbox+"@"+m.From.Domain), query) {
			return true
		}
		if hdr, ok := m.Content.Headers["From"]; ok {
			for _, from := range hdr {
				if strings.Contains(strings.ToLower(from), query) {
					return true
				}
			}
		}
	case "containing":
		if strings.Contains(strings.ToLower(m.Content.Body), query) {
			return true
		}
		for _, hdr := range m.Content.Headers {
			for _, v := range hdr {
				if strings.Contains(strings.ToLower(v), query) {
					return true
				}
			}
		}
	}

	return false
}

// List lists stored messages by index, newest first.
func (memory *InMemory) List(start int, limit int) (*data.Messages, error) {
	messages := make([]data.Message, 0)
	if start < 0 {
		start = 0
	}
	if limit <= 0 {
		msgs := data.Messages(messages)
		return &msgs, nil
	}

	memory.mu.RLock()
	entry := memory.newest
	for i := 0; i < start && entry != nil; i++ {
		entry = entry.prev
	}
	for entry != nil && len(messages) < limit {
		messages = append(messages, *entry.message)
		entry = entry.prev
	}
	memory.mu.RUnlock()

	msgs := data.Messages(messages)
	return &msgs, nil
}

// DeleteOne deletes an individual message by storage ID.
func (memory *InMemory) DeleteOne(id string) error {
	memory.mu.Lock()
	defer memory.mu.Unlock()

	entry, ok := memory.byID[id]
	if !ok {
		return errors.New("message not found")
	}
	memory.remove(entry)
	return nil
}

// DeleteAll deletes all in memory messages.
func (memory *InMemory) DeleteAll() error {
	memory.mu.Lock()
	defer memory.mu.Unlock()

	memory.oldest = nil
	memory.newest = nil
	memory.byID = make(map[string]*messageEntry)
	memory.count = 0
	memory.bytes = 0
	return nil
}

// Load returns an individual message by storage ID.
func (memory *InMemory) Load(id string) (*data.Message, error) {
	memory.mu.RLock()
	defer memory.mu.RUnlock()

	if entry, ok := memory.byID[id]; ok {
		return entry.message, nil
	}
	return nil, nil
}

// estimateMessageSize is deliberately conservative. Most strings in a parsed
// message are substrings of Raw.Data, so counting Raw.Data once avoids
// overestimating the payload by an order of magnitude.
func estimateMessageSize(m *data.Message) int64 {
	const messageOverhead = int64(512)

	size := messageOverhead + int64(len(m.ID))
	if m.From != nil {
		size += int64(len(m.From.Mailbox) + len(m.From.Domain) + len(m.From.Params))
	}
	for _, to := range m.To {
		if to != nil {
			size += int64(len(to.Mailbox) + len(to.Domain) + len(to.Params))
		}
	}

	if m.Raw != nil {
		size += int64(len(m.Raw.From) + len(m.Raw.To)*16 + len(m.Raw.Data) + len(m.Raw.Helo))
	}

	if m.Content != nil {
		if m.Raw == nil || len(m.Raw.Data) == 0 {
			size += int64(len(m.Content.Body))
		}
		for name, values := range m.Content.Headers {
			size += int64(len(name))
			for _, value := range values {
				size += int64(len(value))
			}
		}
	}

	if m.MIME != nil {
		for _, part := range m.MIME.Parts {
			if part == nil {
				continue
			}
			size += 256
			for name, values := range part.Headers {
				size += int64(len(name))
				for _, value := range values {
					size += int64(len(value))
				}
			}
		}
	}

	return size
}
