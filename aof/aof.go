package aof

import (
	"context"
	"go-redis/config"
	"go-redis/interface/database"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
	"os"
	"strconv"
	"sync"
)

const affBufferSize = 1 << 16

type payload struct {
	cmdLine database.CmdLine
	dbIndex int
}

type AofHandler struct {
	ctx         context.Context
	cancel      context.CancelFunc
	db          database.Datebase
	tmpDBMaker  func() database.Datebase
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	aofFsync    string
	aofFinished chan struct{}
	pausingAof  sync.Mutex
	currentDB   int
}

// NewAofHandler
func NewAofHandler(database database.Datebase) (*AofHandler, error) {
	handler := &AofHandler{}
	handler.aofFilename = config.Properties.AppendFilename
	handler.db = database
	handler.LoadAof()
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	handler.aofChan = make(chan *payload, affBufferSize)
	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

// Add payload(set k v)-> aofChan
func (handler *AofHandler) AddAof(dbIndex int, cmd database.CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdLine: cmd,
			dbIndex: dbIndex,
		}
	}
}

// handleAOf payload(set k v)<- aofChan(落盘)
func (handler *AofHandler) handleAof() {
	handler.currentDB = 0
	for p := range handler.aofChan {
		if p.dbIndex != handler.currentDB {
			data := reply.MakeMultiBulkReply(utils.ToCmdLine("select", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Error(err)
				continue
			}
			handler.currentDB = p.dbIndex
		}
		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Error(err)
		}
	}
}

// LoadAof
func (handler *AofHandler) LoadAof() {
	aofFile, err := os.Open(handler.aofFilename)
	if err != nil {
		logger.Error(err)
		return
	}
	defer aofFile.Close()
	ch := parser.ParseStream(aofFile)
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error(p.Err)
			continue
		}
		if p.Data == nil {
			logger.Error("empty data")
		}
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("need multi mulk")
			continue
		}
		fackConn := &connection.Connection{}
		response := handler.db.Exec(fackConn, r.Args)
		if reply.IsErrReply(response) {
			logger.Error("exec err:", response.ToBytes())
		}
	}
}
