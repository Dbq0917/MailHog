package smtp

import (
	"errors"
	"testing"

	"github.com/mailhog/data"
	"github.com/mailhog/storage"
)

type fakeRw struct {
	_read  func(p []byte) (n int, err error)
	_write func(p []byte) (n int, err error)
	_close func() error
}

func (rw *fakeRw) Read(p []byte) (n int, err error) {
	if rw._read != nil {
		return rw._read(p)
	}
	return 0, nil
}

func (rw *fakeRw) Close() error {
	if rw._close != nil {
		return rw._close()
	}
	return nil
}

func (rw *fakeRw) Write(p []byte) (n int, err error) {
	if rw._write != nil {
		return rw._write(p)
	}
	return len(p), nil
}

func TestAccept(t *testing.T) {
	frw := &fakeRw{}
	mChan := make(chan *data.Message, 1)
	Accept("1.1.1.1:11111", frw, storage.CreateInMemory(), mChan, "localhost", nil)
}

func TestSocketError(t *testing.T) {
	frw := &fakeRw{
		_read: func(p []byte) (n int, err error) {
			return -1, errors.New("OINK")
		},
	}
	mChan := make(chan *data.Message, 1)
	Accept("1.1.1.1:11111", frw, storage.CreateInMemory(), mChan, "localhost", nil)
}

func TestAcceptMessage(t *testing.T) {
	mbuf := "EHLO localhost\r\nMAIL FROM:<test>\r\nRCPT TO:<test>\r\nDATA\r\nHi.\r\n.\r\nQUIT\r\n"
	var rbuf []byte
	frw := &fakeRw{
		_read: func(p []byte) (n int, err error) {
			if len(mbuf) == 0 {
				return 0, nil
			}
			n = len(p)
			if n > len(mbuf) {
				n = len(mbuf)
			}
			copy(p, mbuf[:n])
			mbuf = mbuf[n:]
			return n, nil
		},
		_write: func(p []byte) (n int, err error) {
			rbuf = append(rbuf, p...)
			return len(p), nil
		},
		_close: func() error {
			return nil
		},
	}

	mChan := make(chan *data.Message, 1)
	store := storage.CreateInMemory()
	Accept("1.1.1.1:11111", frw, store, mChan, "localhost", nil)

	if got := store.Count(); got != 1 {
		t.Fatalf("stored %d messages, want 1", got)
	}
	select {
	case message := <-mChan:
		if message == nil {
			t.Fatal("received nil message notification")
		}
	default:
		t.Fatal("message notification was not sent")
	}
}

func TestValidateAuthentication(t *testing.T) {
	c := &Session{}

	err, ok := c.validateAuthentication("OINK")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("authentication should be accepted")
	}

	err, ok = c.validateAuthentication("OINK", "arg1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("authentication should be accepted")
	}

	err, ok = c.validateAuthentication("OINK", "arg1", "arg2")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("authentication should be accepted")
	}
}

func TestValidateRecipient(t *testing.T) {
	c := &Session{}

	if !c.validateRecipient("OINK") {
		t.Fatal("recipient should be accepted")
	}
	if !c.validateRecipient("foo@bar.mailhog") {
		t.Fatal("recipient should be accepted")
	}
}

func TestValidateSender(t *testing.T) {
	c := &Session{}

	if !c.validateSender("OINK") {
		t.Fatal("sender should be accepted")
	}
	if !c.validateSender("foo@bar.mailhog") {
		t.Fatal("sender should be accepted")
	}
}
