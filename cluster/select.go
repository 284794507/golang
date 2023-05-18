package cluster

import "go-redis/interface/resp"

func selectFunc(cluster *ClusterDateBase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	return cluster.db.Exec(c, cmdArgs)
}
