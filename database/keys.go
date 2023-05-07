package database

import (
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/lib/wildcard"
	"go-redis/resp/reply"
)

// del exist keys flush type rename renamenx
func Del(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	deleted := db.Removes(keys...)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("del", args...))
	}
	return reply.MakeIntReply(int64(deleted))
}

func exists(db *DB, args [][]byte) resp.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		_, ok := db.GetEntity(key)
		if ok {
			result++
		}
	}
	return reply.MakeIntReply(result)
}

func flushDB(db *DB, args [][]byte) resp.Reply {
	db.Flush()
	db.addAof(utils.ToCmdLine3("flushdb", args...))
	return reply.MakeOkReply()
}

func execType(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	val, ok := db.GetEntity(key)
	if ok {
		switch val.Data.(type) {
		case []byte:
			return reply.MakeStatusReply("string")
			//TODO
		}
		return reply.UnKonwErrReply{}
	}
	return reply.MakeStatusReply("none")
}

func rename(db *DB, args [][]byte) resp.Reply {
	k1, k2 := string(args[0]), string(args[1])
	val, ok := db.GetEntity(k1)
	if ok {
		db.PutEntity(k2, val)
		db.Remove(k1)
		db.addAof(utils.ToCmdLine3("rename", args...))
		return reply.MakeOkReply()
	}
	return reply.MakeStandardErrReply("no such key")
}

func renamenx(db *DB, args [][]byte) resp.Reply {
	k1, k2 := string(args[0]), string(args[1])
	_, ok := db.GetEntity(k2)
	if ok {
		return reply.MakeIntReply(0)
	}
	val, ok := db.GetEntity(k1)
	if ok {
		db.PutEntity(k2, val)
		db.Remove(k1)
		db.addAof(utils.ToCmdLine3("renamenx", args...))
		return reply.MakeIntReply(1)
	}
	return reply.MakeStandardErrReply("no such key")
}

func execKeys(db *DB, args [][]byte) resp.Reply {
	pattern, err := wildcard.CompilePattern(string(args[0]))
	if err != nil {
		return reply.MakeStandardErrReply("pattern key error")
	}
	result := make([][]byte, 0)
	db.data.ForEach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) {
			result = append(result, []byte(key))
		}
		return true
	})
	return reply.MakeMultiBulkReply(result)
}

func init() {
	RegisterCommand("del", Del, -2)
	RegisterCommand("exists", exists, -2)
	RegisterCommand("flush", flushDB, 1)
	RegisterCommand("type", execType, 2)
	RegisterCommand("rename", rename, 3)
	RegisterCommand("renamenx", renamenx, 3)
	RegisterCommand("keys", execKeys, 2)
}
