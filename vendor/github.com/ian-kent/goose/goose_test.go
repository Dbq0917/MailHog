package goose

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func TestNotifyDoesNotBlockOnSlowReceiver(t *testing.T) {
	stream := NewEventStream()
	server, client := net.Pipe()
	defer client.Close()

	receiver := &EventReceiver{
		stream: stream,
		conn:   server,
		bufrw:  bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server)),
		send:   make(chan streamEvent, 1),
		done:   make(chan struct{}),
	}
	stream.receivers[server] = receiver
	receiver.send <- streamEvent{}

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			stream.Notify("data", []byte("message"))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Notify blocked on a slow receiver")
	}

	if !stream.HasReceivers() {
		t.Fatal("receiver should still be registered")
	}
	receiver.close()
	if stream.HasReceivers() {
		t.Fatal("closed receiver is still registered")
	}
}
