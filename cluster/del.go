package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

func delFunc(cluster *ClusterDateBase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	replies := cluster.broadcast(c, cmdArgs)
	var errReply reply.ErrorReply
	var delNum int64 = 0
	for _, r := range replies {
		if reply.IsErrReply(r) {
			errReply = r.(reply.ErrorReply)
			break
		} else {
			intReply, ok := r.(*reply.IntReply)
			if !ok {
				errReply = reply.MakeStandardErrReply("error")
			} else {
				delNum += intReply.Code
			}
		}
	}
	if errReply == nil {
		return reply.MakeIntReply(delNum)
	}
	return reply.MakeStandardErrReply("error:" + errReply.Error())
}
