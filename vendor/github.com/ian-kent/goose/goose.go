package goose

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
)

var (
	// ErrUnableToHijackRequest is returned by AddReceiver if the type
	// conversion to http.Hijacker is unsuccessful
	ErrUnableToHijackRequest = errors.New("Unable to hijack request")
)

// EventStream represents a collection of receivers
type EventStream struct {
	mutex     *sync.RWMutex
	receivers map[net.Conn]*EventReceiver
}

// NewEventStream creates a new event stream
func NewEventStream() *EventStream {
	return &EventStream{
		mutex:     new(sync.RWMutex),
		receivers: make(map[net.Conn]*EventReceiver),
	}
}

const receiverQueueSize = 8

type streamEvent struct {
	size string
	data string
}

// EventReceiver represents a hijacked HTTP connection
type EventReceiver struct {
	stream    *EventStream
	conn      net.Conn
	bufrw     *bufio.ReadWriter
	send      chan streamEvent
	done      chan struct{}
	closeOnce sync.Once
}

// HasReceivers reports whether any event stream clients are connected.
func (es *EventStream) HasReceivers() bool {
	es.mutex.RLock()
	defer es.mutex.RUnlock()
	return len(es.receivers) > 0
}

// Notify sends the event to all event stream receivers
func (es *EventStream) Notify(event string, bytes []byte) {
	lines := strings.Split(string(bytes), "\n")

	data := ""
	for _, l := range lines {
		data += event + ": " + l + "\n"
	}

	sz := len(data) + 1
	size := fmt.Sprintf("%X", sz)

	ev := streamEvent{size: size, data: data}

	es.mutex.RLock()
	defer es.mutex.RUnlock()
	for _, er := range es.receivers {
		// Notifications are best-effort. Dropping an event for a slow client
		// keeps memory bounded and never applies backpressure to ingestion.
		select {
		case er.send <- ev:
		default:
		}
	}
}

func (er *EventReceiver) run() {
	for {
		select {
		case ev := <-er.send:
			if err := er.writeEvent(ev); err != nil {
				er.close()
				return
			}
		case <-er.done:
			return
		}
	}
}

func (er *EventReceiver) writeEvent(ev streamEvent) error {
	_, err := er.write([]byte(ev.size + "\r\n"))
	if err != nil {
		return err
	}

	lines := strings.Split(ev.data, "\n")
	for _, ln := range lines {
		_, err = er.write([]byte(ln + "\n"))
		if err != nil {
			return err
		}
	}
	_, err = er.write([]byte("\r\n"))
	return err
}

func (er *EventReceiver) write(bytes []byte) (int, error) {
	n, err := er.bufrw.Write(bytes)

	if err != nil {
		return n, err
	}

	err = er.bufrw.Flush()
	return n, err
}

func (er *EventReceiver) close() {
	er.closeOnce.Do(func() {
		er.stream.mutex.Lock()
		if er.stream.receivers[er.conn] == er {
			delete(er.stream.receivers, er.conn)
		}
		er.stream.mutex.Unlock()
		close(er.done)
		er.conn.Close()
	})
}

// AddReceiver hijacks a http.ResponseWriter and attaches it to the event stream
func (es *EventStream) AddReceiver(w http.ResponseWriter) (*EventReceiver, error) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	w.WriteHeader(200)

	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, ErrUnableToHijackRequest
	}

	hjConn, hjBufrw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}

	rec := &EventReceiver{
		stream: es,
		conn:   hjConn,
		bufrw:  hjBufrw,
		send:   make(chan streamEvent, receiverQueueSize),
		done:   make(chan struct{}),
	}

	es.mutex.Lock()
	es.receivers[hjConn] = rec
	es.mutex.Unlock()
	go rec.run()

	return rec, nil
}
