package cluster

import (
	"context"
	pool "github.com/jolestar/go-commons-pool"
	"go-redis/config"
	database2 "go-redis/database"
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/consistenthash"
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
	for _, peer := range config.Properties.Peers {
		pool.NewObjectPoolWithDefaultConfig(ctx, &connnectionFactory{
			Peer: peer,
		})
	}
	cluster.nodes = nodes

	return cluster
}

func (c *ClusterDateBase) Exec(client resp.Connection, args [][]byte) resp.Reply {
	//TODO implement me
	panic("implement me")
}

func (c *ClusterDateBase) Close() {
	//TODO implement me
	panic("implement me")
}

func (c *ClusterDateBase) AfterClientClose(conn resp.Connection) {
	//TODO implement me
	panic("implement me")
}
