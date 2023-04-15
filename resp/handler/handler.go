package handler

import (
	"context"
	"go-redis/database"
	idatabase "go-redis/interface/database"
	"go-redis/lib/sync/atomic"
	"go-redis/resp/connection"
	"net"
	"sync"
)

var (
	unknowErrReplyBytes = []byte("-ERR unkown\r\n")
)

type RespHandler struct {
	activeConn sync.Map
	db         idatabase.Datebase
	cloing     atomic.Boolean
}

func MakeHandler() *RespHandler {
	var db idatabase.Datebase
	db = database.NewDatabase()
	return &RespHandler{
		db: db,
	}
}

func (r *RespHandler) closeClient(client *connection.Connection) {

}

func (r *RespHandler) Close() error {
	//TODO implement me
	panic("implement me")
}

func (r *RespHandler) Handle(ctx context.Context, conn net.Conn) {

}
