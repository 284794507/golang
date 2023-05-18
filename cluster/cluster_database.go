package cluster

import (
	"context"
	pool "github.com/jolestar/go-commons-pool"
	"go-redis/config"
	database2 "go-redis/database"
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/consistenthash"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"strings"
)

type ClusterDateBase struct {
	self string

	nodes          []string
	peerPicker     *consistenthash.NodeMap
	peerConnection map[string]*pool.ObjectPool
	db             database.Datebase
}

func MakeClusterDatabase() *ClusterDateBase {
	cluster := &ClusterDateBase{
		self:           config.Properties.Self,
		db:             database2.NewDatabase(),
		peerPicker:     consistenthash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool),
	}

	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	nodes = append(nodes, config.Properties.Self)
	nodes = append(nodes, config.Properties.Peers...)
	cluster.peerPicker.AddNode(nodes...)
	ctx := context.Background()
	peerMap := make(map[string]*pool.ObjectPool)
	for _, peer := range config.Properties.Peers {
		peerPool := pool.NewObjectPoolWithDefaultConfig(ctx, &connnectionFactory{
			Peer: peer,
		})
		peerMap[peer] = peerPool
	}
	cluster.peerConnection = peerMap
	cluster.nodes = nodes

	return cluster
}

type CmdFunc func(cluster *ClusterDateBase, c resp.Connection, cmdArgs [][]byte) resp.Reply

var router = makeRouter()

func (c *ClusterDateBase) Exec(client resp.Connection, args [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
			result = reply.UnKonwErrReply{}
		}
	}()

	execFunc, ok := router[strings.ToLower(string(args[0]))]
	if !ok {
		return reply.MakeStatusReply("not supported cmd")
	}
	result = execFunc(c, client, args)
	return
}

func (c *ClusterDateBase) Close() {
	c.db.Close()
}

func (c *ClusterDateBase) AfterClientClose(conn resp.Connection) {
	c.db.AfterClientClose(conn)
}
