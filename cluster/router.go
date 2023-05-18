package cluster

import "go-redis/interface/resp"

func makeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)
	routerMap["exists"] = defaultFunc
	routerMap["type"] = defaultFunc
	routerMap["set"] = defaultFunc
	routerMap["setnx"] = defaultFunc
	routerMap["get"] = defaultFunc
	routerMap["getset"] = defaultFunc
	routerMap["ping"] = pingFunc
	routerMap["renamenx"] = renameFunc
	routerMap["rename"] = renameFunc
	routerMap["flushdb"] = flushFunc
	routerMap["del"] = delFunc
	routerMap["select"] = selectFunc

	return routerMap
}

func defaultFunc(cluster *ClusterDateBase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	curKey := string(cmdArgs[1])
	peer := cluster.peerPicker.PickNode(curKey)
	return cluster.relay(peer, c, cmdArgs)
}
