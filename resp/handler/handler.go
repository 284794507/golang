package handler

import (
	"context"
	"fmt"
	"go-redis/cluster"
	"go-redis/config"
	"go-redis/database"
	idatabase "go-redis/interface/database"
	"go-redis/lib/logger"
	"go-redis/lib/sync/atomic"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
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
	if config.Properties.Self != "" && len(config.Properties.Peers) != 0 {
		db = cluster.MakeClusterDatabase()
	} else {
		db = database.NewDatabase()
	}
	return &RespHandler{
		db: db,
	}
}

func (r *RespHandler) closeClient(client *connection.Connection) {
	logger.Info("connection close:" + client.RemoteAddr().String())
}

func (r *RespHandler) Close() error {
	//TODO implement me
	panic("implement me")
}

func (r *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	fmt.Println("RespHandler")
	if r.cloing.Get() {
		_ = conn.Close()
	}
	client := connection.NewConn(conn)
	r.activeConn.Store(client, struct{}{})
	for chPayload := range parser.ParseStream(conn) {
		fmt.Println("RespHandler chPayload:", chPayload)
		if chPayload.Err != nil {
			logger.Error(chPayload.Err)
			if chPayload.Err == io.EOF || chPayload.Err == io.ErrUnexpectedEOF {
				r.closeClient(client)
				return
			}
			errReply := reply.MakeStandardErrReply(chPayload.Err.Error())
			_, err := client.Write(errReply.ToBytes())
			if err != nil {
				r.closeClient(client)
				return
			}
		}
		if chPayload.Data == nil {
			continue
		}
		var newArgs [][]byte
		switch curReply := chPayload.Data.(type) {
		case *reply.StatusReply:
			newArgs = append(newArgs, ([]byte)(curReply.Status))
		case *reply.MultiBulkReply:

			newArgs = curReply.Args
		default:
			logger.Error("require multi bulk reply")
			continue
		}
		for _, arg := range newArgs {
			fmt.Println("RespHandler arg:", string(arg))
		}
		result := r.db.Exec(client, newArgs)
		if result != nil {
			fmt.Println("RespHandler result:", string(result.ToBytes()))
			client.Write(result.ToBytes())
		} else {
			client.Write(unknowErrReplyBytes)
		}
	}
}
