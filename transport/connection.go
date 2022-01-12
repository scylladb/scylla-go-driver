package transport

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"scylla-go-driver/frame"
	"scylla-go-driver/frame/request"
	"scylla-go-driver/frame/response"
	"strconv"
)

type Connection struct {
	socket net.Conn
	reader *bufio.Reader

	newTask                 chan ConnRequest
	streamsHandler          chan streamHandlerMsg
	streamIDReceiver        chan uint16
	responseChannelReceiver chan (chan ConnResponse)

	defaultConsistency frame.Consistency
}

type requester interface {
	WriteTo(b *frame.Buffer)
}

type ConnRequest struct {
	Body     requester
	Code     frame.OpCode
	Response chan ConnResponse
}

type ConnResponse struct {
	Header frame.Header
	Body   []byte
}

// SendOptions sends Options request and waits for response Supported frame.
// Not suer what to do with errors. I don't think that creating separate struct
// for every response frame seems like a little too much.
func (c *Connection) SendOptions() response.Supported {
	res := make(chan ConnResponse)
	req := ConnRequest{
		Body:     request.Options{},
		Code:     frame.OpOptions,
		Response: res,
	}
	c.newTask <- req

	f := <-res
	buf := frame.NewBuffer(f.Body)

	switch f.Header.Opcode {
	case frame.OpError:
		err := response.ParseError(&buf)
		fmt.Println(err.Message)
		return response.Supported{}
	case frame.OpSupported:
		return response.ParseSupported(&buf)
	default:
		fmt.Println("invalid response frame opcode for Options request")
		return response.Supported{}
	}
}

func (c *Connection) Query(query string) chan ConnResponse {
	response := make(chan ConnResponse)
	req := ConnRequest{
		Body: request.Query{
			Query:       query,
			Consistency: c.defaultConsistency,
		},
		Code:     frame.OpQuery,
		Response: response,
	}
	c.newTask <- req
	return response
}

func (c *Connection) allocStreamId(ch chan ConnResponse) uint16 {
	msg := streamHandlerMsg{status: requestNewID, channel: ch}
	c.streamsHandler <- msg
	newID := <-c.streamIDReceiver
	return newID
}

func (c *Connection) GetResponseChannelById(id uint16) chan ConnResponse {
	msg := streamHandlerMsg{status: getChannel, id: id}
	c.streamsHandler <- msg
	ch := <-c.responseChannelReceiver
	return ch
}

// NewDialerConnection creates connection from specific local port.
func NewDialerConnection(remoteAddr string, localPort int) (*Connection, error) {
	localAddr, _ := net.ResolveTCPAddr("tcp", "172.17.0.1:"+strconv.Itoa(localPort))
	socket, _ := (&net.Dialer{LocalAddr: localAddr}).Dial("tcp", remoteAddr)
	reader := bufio.NewReader(socket)

	requestChan := make(chan ConnRequest)
	streamHandler := make(chan streamHandlerMsg)
	writeReceiver := make(chan uint16)
	readReceiver := make(chan (chan ConnResponse))

	conn := &Connection{
		socket:                  socket,
		reader:                  reader,
		newTask:                 requestChan,
		streamsHandler:          streamHandler,
		streamIDReceiver:        writeReceiver,
		responseChannelReceiver: readReceiver,
		defaultConsistency:      frame.ONE,
	}

	if err := initConnection(conn); err != nil {
		return &Connection{}, err
	}

	go streamsHandler(streamHandler, writeReceiver, readReceiver)
	go readTask(conn)
	go writeTask(conn)

	return conn, nil
}

func NewConnection(addr string) (*Connection, error) {
	return NewDialerConnection(addr, -1)
}

func sendStartup(connection *Connection) error {
	buf := &frame.Buffer{}

	header := frame.Header{
		Version: 0x04,
		Opcode:  frame.OpStartup,
	}
	header.WriteTo(buf)

	startup := request.Startup{
		Options: frame.StartupOptions{
			"CQL_VERSION": "3.0.0",
		},
	}
	startup.WriteTo(buf)
	buf.WriteBodyLength()

	_, err := connection.socket.Write(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func isReady(connection *Connection) error {
	buf := make([]byte, 9)

	for {
		if _, err := io.ReadFull(connection.reader, buf); err != nil {
			return err
		}

		header, err := frame.ParseHeaderRaw(buf)
		if err != nil {
			return err
		}

		if header.Opcode == frame.OpReady {
			return nil
		}
	}
}

func initConnection(conn *Connection) error {
	if err := sendStartup(conn); err != nil {
		return err
	}
	return isReady(conn)
}

func readTask(connection *Connection) {
	buf := make([]byte, 9)
	reader := bufio.NewReader(connection.socket)
	for {
		if _, err := io.ReadFull(reader, buf); err != nil {
			return
		}
		header, err := frame.ParseHeaderRaw(buf)
		if err != nil {
			// How to handle invalid header?
			// Can we assume that the header length is correct
			// and read that many bytes to erase trash?
			continue
		}

		body := make([]byte, header.Length)
		if _, err = io.ReadFull(reader, body); err != nil {
			return
		}

		res := ConnResponse{
			Header: header,
			Body:   body,
		}

		response := connection.GetResponseChannelById(res.Header.StreamID)
		response <- res
	}
}

func writeTask(connection *Connection) {
	for {
		req, ok := <-connection.newTask
		if !ok {
			return
		}

		id := connection.allocStreamId(req.Response)

		buf := frame.Buffer{}
		header := frame.Header{
			Version:  0x04,
			StreamID: id,
			Opcode:   req.Code,
		}

		header.WriteTo(&buf)
		req.Body.WriteTo(&buf)
		buf.WriteBodyLength()

		_, err := connection.socket.Write(buf.Bytes())
		if err != nil {
			fmt.Println("writeTask error")
		}
	}
}

type streamHandlerMsgStatus uint8

const (
	requestNewID streamHandlerMsgStatus = iota
	getChannel
)

type streamHandlerMsg struct {
	status  streamHandlerMsgStatus
	id      uint16
	channel chan ConnResponse
}

func streamsHandler(task chan streamHandlerMsg, senderToWrite chan uint16, senderToRead chan (chan ConnResponse)) {
	nextID := uint16(0)
	streamResponses := make(map[uint16]chan ConnResponse)
	for {
		msg := <-task
		switch msg.status {
		case requestNewID:
			id := nextID
			nextID++
			streamResponses[id] = msg.channel
			senderToWrite <- id

		case getChannel:
			if v, ok := streamResponses[msg.id]; ok {
				senderToRead <- v
				delete(streamResponses, msg.id)
			} else {
				senderToRead <- nil
			}
		}
	}
}
