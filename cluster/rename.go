package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

func renameFunc(cluster *ClusterDateBase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	if len(cmdArgs) != 3 {
		return reply.MakeStandardErrReply("Err Wrong number args")
	}
	srcKey := string(cmdArgs[1])
	desKey := string(cmdArgs[2])
	srcPeer := cluster.peerPicker.PickNode(srcKey)
	desPeer := cluster.peerPicker.PickNode(desKey)

	if srcPeer != desPeer {
		//TODO
	}
	return cluster.relay(srcPeer, c, cmdArgs)
}
