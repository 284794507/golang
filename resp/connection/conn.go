package connection

import (
	"go-redis/lib/logger"
	"go-redis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

const (
	//this is a connection with slave
	flagSlave = uint64(1 << iota)
	//this is a connection with master
	flagMaster
	//this is a connection with a transaction
	flagMulti
)

type Connection struct {
	conn net.Conn

	//wait until finish sending data, used for graceful shutdown
	sendingData wait.Wait

	//lock while server sending response
	mu    sync.Mutex
	flags uint64

	//subscribing channels
	subs map[string]bool

	//password may be changed by config command during runtime, so store the password
	password string

	//queued commands for multi
	queue    [][][]byte
	watching map[string]uint32
	txErrors []error

	//selected db
	selectedDB int
}

var connPool = sync.Pool{
	New: func() interface{} {
		return &Connection{}
	},
}

// RemoteAddr returns the remote network address
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// close disconnect with the client
func (c *Connection) Close() error {
	c.sendingData.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	c.subs = nil
	c.password = ""
	c.queue = nil
	c.watching = nil
	c.txErrors = nil
	c.selectedDB = 0
	connPool.Put(c)
	return nil
}

// NewConn creates Connection instance
func NewConn(conn net.Conn) *Connection {
	c, ok := connPool.Get().(*Connection)
	if !ok {
		logger.Error("connection pool make wrong type")
		return &Connection{
			conn: conn,
		}
	}
	c.conn = conn
	c.selectedDB = 0
	return c
}

// Write sends response to client over tcp connection
func (c *Connection) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.sendingData.Add(1)
	defer func() {
		c.sendingData.Done()
	}()

	return c.conn.Write(b)
}

func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}

func (c *Connection) Name() string {
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return ""
}

// subscribe add current connection into subscribers of the given channel
func (c *Connection) Subscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs == nil {
		c.subs = make(map[string]bool)
	}
	c.subs[channel] = true
}

// unSubscribe removers current connection into subscribers of the given channel
func (c *Connection) UnSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.subs) == 0 {
		return
	}
	delete(c.subs, channel)
}

// subcount returns the number of subscribing channels
func (c *Connection) GetChannels() []string {
	if c.subs == nil {
		return make([]string, 0)
	}
	channels := make([]string, len(c.subs))
	i := 0
	for channel := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}

func (c *Connection) SetPassword(password string) {
	c.password = password
}

func (c *Connection) GetPassord() string {
	return c.password
}

// tells is connection in an uncommitted transaction
func (c *Connection) InMultistatu() bool {
	return c.flags&flagMulti > 0
}

// sets transaction flag
func (c *Connection) SetMultiState(state bool) {
	if !state { //reset data when cancel multi
		c.watching = nil
		c.queue = nil
		c.flags &= ^flagMulti //clean multi flag
		return
	}
	c.flags |= flagMulti
}

// GetQueueCmdLine returns queued commands of current transaction
func (c *Connection) GetQueuedCmdLine() [][][]byte {
	return c.queue
}

// EnqueueCmd enqueues command of current transaction
func (c *Connection) EnqueueCmd(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)
}

// AddTxError storess syntax error within transaction
func (c *Connection) AddTxError(err error) {
	c.txErrors = append(c.txErrors, err)
}

func (c *Connection) GetTxErrors() []error {
	return c.txErrors
}

// ClearQueueCmds clears queued commands of current transaction
func (c *Connection) ClearQueueCmds() {
	c.queue = nil
}

// GetWatching returns watching keys and their version code when started watching
func (c *Connection) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}

func (c *Connection) SetSlave() {
	c.flags |= flagSlave
}

func (c *Connection) IsSlave() bool {
	return c.flags&flagSlave > 0
}

func (c *Connection) SetMaster() {
	c.flags |= flagMaster
}

func (c *Connection) IsMaster() bool {
	return c.flags&flagMaster > 0
}
