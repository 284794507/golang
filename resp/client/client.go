package client

/**
*每个client都是一对一通讯，chan就像一个队列，先进先出
*每次请求发出以后，将请求体指针放到等待回复的队列中
*如果请求正常返回，则通过指针将返回更新到请求体中
 */

import (
	"net"
	"sync"
	"time"
)

import (
	"errors"
	"fmt"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/lib/sync/wait"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"runtime/debug"
	"strings"
	"sync/atomic"
)

const (
	created = iota
	running
	closed
)

// Client is a pipeline mode redis client
type Client struct {
	conn        net.Conn
	pendingReqs chan *request // wait to send
	waitingReqs chan *request // waiting response
	ticker      *time.Ticker
	addr        string
	status      int32

	working *sync.WaitGroup
}

type request struct {
	id        uint64
	args      [][]byte
	reply     resp.Reply
	heartbeat bool
	waiting   *wait.Wait
	err       error
}

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

// MakeClient creates a new client
func MakeClient(addr string) (*Client, error) {
	logger.Info("Dial with:" + addr)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:        addr,
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		working:     &sync.WaitGroup{},
	}, nil
}

// Start starts asynchronous goroutines
func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	go client.handleWrite()
	go client.handleRead()
	go client.heartbeat()
	atomic.StoreInt32(&client.status, running)
}

// Close stops asynchronous goroutines and close connection
func (client *Client) Close() {
	atomic.StoreInt32(&client.status, closed)
	client.ticker.Stop()
	close(client.pendingReqs)
	client.working.Wait()
	_ = client.conn.Close()
	close(client.waitingReqs)
}

func (client *Client) reconnect() {
	logger.Info("reconnect with:" + client.addr)
	_ = client.conn.Close()

	var conn net.Conn
	for i := 0; i < 3; i++ {
		var err error
		conn, err = net.Dial("tcp", client.addr)
		if err != nil {
			logger.Error("reconnect error:" + err.Error())
			time.Sleep(time.Second)
			continue
		} else {
			break
		}
	}
	if conn == nil {
		client.Close()
		return
	}
	client.conn = conn
	close(client.waitingReqs)
	for req := range client.waitingReqs {
		req.err = errors.New("connection closed")
		req.waiting.Done()
	}
	client.waitingReqs = make(chan *request, chanSize)
	go client.handleRead()
}

func (client *Client) handleConnectionError(err error) error {
	err1 := client.conn.Close()
	if err1 != nil {
		if opErr, ok := err1.(*net.OpError); ok {
			if opErr.Err.Error() != "use of closed network connection" {
				return opErr.Err
			} else {
				return err1
			}
		}
	}
	conn, err1 := net.Dial("tcp", client.addr)
	if err1 != nil {
		logger.Error(err1)
		return err1
	}
	client.conn = conn
	go func() {
		client.handleRead()
	}()
	return nil
}

func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat()
	}
}

func (client *Client) doHeartbeat() {
	request := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- request
	request.waiting.WaitWithTimeout(maxWait)
}

func (client *Client) handleWrite() {
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	fmt.Println("Client doRequest:", req)
	for _, arg := range req.args {
		fmt.Println("Client doRequest arg:", string(arg))
	}
	re := reply.MakeMultiBulkReply(req.args)
	bytes := re.ToBytes()
	var err error
	for i := 0; i < 3; i++ {
		_, err = client.conn.Write(bytes)
		fmt.Println("Client err:", err)
		if err == nil ||
			(!strings.Contains(err.Error(), "timeout") &&
				!strings.Contains(err.Error(), "deadline exceeded")) {
			break
		}
	}
	if err == nil {
		client.waitingReqs <- req
	} else {
		req.err = err
		req.waiting.Done()
	}
}

func (client *Client) finishRequest(reply resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()
	fmt.Println("Client finishRequest reply:", reply)
	request := <-client.waitingReqs
	if request == nil {
		return
	}
	request.reply = reply
	fmt.Println("Client finishRequest request:", request)
	if request.waiting != nil {
		request.waiting.Done()
	}
}

func (client *Client) handleRead() {
	ch := parser.ParseStream(client.conn)
	for payload := range ch {
		fmt.Println("Client handleRead payload:", payload)
		if payload.Err != nil {
			status := atomic.LoadInt32(&client.status)
			if status == closed {
				return
			}
			client.reconnect()
			return
		}
		client.finishRequest(payload.Data)
	}
}

// Send sends a request to redis server
func (client *Client) Send(args [][]byte) resp.Reply {
	if atomic.LoadInt32(&client.status) != running {
		return reply.MakeStandardErrReply("client closed")
	}
	request := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()

	fmt.Println("Client Send request1:", request)
	for _, arg := range request.args {
		fmt.Println("Client Send arg:", string(arg))
	}
	client.pendingReqs <- request
	timeout := request.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return reply.MakeStandardErrReply("server time out")
	}
	if request.err != nil {
		return reply.MakeStandardErrReply("request failed")
	}
	fmt.Println("Client Send request2:", request)
	return request.reply
}
