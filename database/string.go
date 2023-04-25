package database

import (
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// get set setnx getset strlen
func execGet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	result := entity.Data
	bytes := result.([]byte)
	return reply.MakeBulkReply(bytes)
}

func execSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	val := args[1]
	entity := &database.DataEntity{
		Data: val,
	}
	db.PutEntity(key, entity)
	return reply.MakeOkReply()
}

func execSetnx(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	val := args[1]
	entity := &database.DataEntity{
		Data: val,
	}
	result := db.PutIfAbsent(key, entity)
	return reply.MakeIntReply(int64(result))
}

func execGetSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	val := args[1]
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	db.PutIfAbsent(key, &database.DataEntity{
		Data: val,
	})
	bytes := entity.Data.([]byte)
	return reply.MakeBulkReply(bytes)
}

func execStrLen(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	bytes := entity.Data.([]byte)
	return reply.MakeIntReply(int64(len(bytes)))
}

func init() {
	RegisterCommand("get", execGet, 2)
	RegisterCommand("set", execSet, 3)
	RegisterCommand("setnx", execSetnx, 3)
	RegisterCommand("getSet", execGetSet, 3)
	RegisterCommand("strLen", execStrLen, 2)
}
