package transport

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"runtime"
	"sync"

	"scylla-go-driver/frame"
	. "scylla-go-driver/frame/request"
	. "scylla-go-driver/frame/response"
)

// TODO on send and recv i/o error we shall reset the connection
// TODO request coelasting if there is more items in requestCh than we can send them together, we can check channel length, we need a write buffer

type response struct {
	frame.Header
	frame.Response
	Err error
}

type responseHandler chan response

type request struct {
	frame.Request
	StreamID        frame.StreamID
	Compress        bool
	Tracing         bool
	ResponseHandler responseHandler
}

type connWriter struct {
	conn      io.Writer
	buf       frame.Buffer
	requestCh chan request
}

func (c *connWriter) submit(r request) {
	c.requestCh <- r
}

func (c *connWriter) loop() {
	runtime.LockOSThread()

	for {
		r, ok := <-c.requestCh
		if !ok {
			return
		}

		if err := c.send(r); err != nil {
			r.ResponseHandler <- response{Err: fmt.Errorf("send: %w", err)}
		}
	}
}

func (c *connWriter) send(r request) error {
	c.buf.Reset()

	// Dump request with header to buffer
	h := frame.Header{
		Version:  frame.CQLv4,
		StreamID: r.StreamID,
		OpCode:   r.OpCode(),
	}
	h.WriteTo(&c.buf)
	r.WriteTo(&c.buf)

	// Update length in header
	b := c.buf.Bytes()
	l := uint32(len(b) - frame.HeaderSize)
	binary.BigEndian.PutUint32(b[5:9], l)

	// Send
	if _, err := frame.CopyBuffer(&c.buf, c.conn); err != nil {
		return err
	}

	return nil
}

type connReader struct {
	conn *bufio.Reader
	buf  frame.Buffer
	bufw io.Writer

	h map[frame.StreamID]responseHandler
	s streamIDAllocator
	// mu guards h and s.
	mu sync.Mutex
}

func (c *connReader) setHandler(h responseHandler) (frame.StreamID, error) {
	c.mu.Lock()
	streamID, err := c.s.Alloc()
	if err != nil {
		c.mu.Unlock()
		return 0, fmt.Errorf("stream ID alloc: %w", err)
	}

	c.h[streamID] = h
	c.mu.Unlock()
	return streamID, err
}

func (c *connReader) handler(streamID frame.StreamID) responseHandler {
	c.mu.Lock()
	h := c.h[streamID]
	c.mu.Unlock()
	return h
}

func (c *connReader) loop() {
	runtime.LockOSThread()

	c.bufw = frame.BufferWriter(&c.buf)
	for {
		resp := c.recv()
		if h := c.handler(resp.StreamID); h != nil {
			h <- resp
		} else {
			// FIXME gracefully handle recv error
			log.Fatalf("recv error: %+v, %+v", resp.Header, resp.Response)
		}
	}
}

func (c *connReader) recv() response {
	c.buf.Reset()

	var r response

	// Read header
	if _, err := io.CopyN(c.bufw, c.conn, frame.HeaderSize); err != nil {
		r.Err = fmt.Errorf("read header: %w", err)
		return r
	}
	r.Header = frame.ParseHeader(&c.buf)
	if err := c.buf.Error(); err != nil {
		r.Err = fmt.Errorf("parse header: %w", err)
		return r
	}

	// Read body
	if _, err := io.CopyN(c.bufw, c.conn, int64(r.Header.Length)); err != nil {
		r.Err = fmt.Errorf("read body: %w", err)
		return r
	}
	r.Response = c.parse(r.Header.OpCode)
	if err := c.buf.Error(); err != nil {
		r.Err = fmt.Errorf("parse body: %w", err)
		return r
	}

	return r
}

func (c *connReader) parse(op frame.OpCode) frame.Response {
	// TODO add all responses
	switch op {
	case frame.OpError:
		return ParseError(&c.buf)
	case frame.OpReady:
		return ParseReady(&c.buf)
	default:
		log.Fatalf("not supported %d", op)
		return nil
	}
}

type Conn struct {
	conn net.Conn
	w    connWriter
	r    connReader
}

const (
	requestChanSize = 1024
	ioBufferSize    = 8192
)

func WrapConn(conn net.Conn) *Conn {
	c := &Conn{
		conn: conn,
		w: connWriter{
			conn:      conn,
			requestCh: make(chan request, requestChanSize),
		},
		r: connReader{
			conn: bufio.NewReaderSize(conn, ioBufferSize),
			h:    make(map[frame.StreamID]responseHandler),
		},
	}
	go c.w.loop()
	go c.r.loop()

	return c
}

// TODO add conn Close, make sure go routines exit

func (c *Conn) Startup(options frame.StartupOptions) (frame.Response, error) {
	return c.sendRequest(&Startup{Options: options}, false, false)
}

func (c *Conn) sendRequest(req frame.Request, compress, tracing bool) (frame.Response, error) {
	h := make(responseHandler)

	streamID, err := c.r.setHandler(h)
	if err != nil {
		return nil, fmt.Errorf("set handler: %w", err)
	}

	r := request{
		Request:         req,
		StreamID:        streamID,
		Compress:        compress,
		Tracing:         tracing,
		ResponseHandler: h,
	}

	c.w.submit(r)

	resp := <-h
	c.r.s.Free(streamID)

	return resp.Response, resp.Err
}
