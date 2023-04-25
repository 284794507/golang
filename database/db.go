package database

import (
	"go-redis/datastruct/dict"
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
	"strings"
)

type DB struct {
	index int
	data  dict.Dict
}

type ExecFunc func(db *DB, args [][]byte) resp.Reply
type CmdLine = [][]byte

func makeDB() *DB {
	db := &DB{
		data: dict.MakeSyncDict(),
	}
	return db
}

func (db *DB) Exec(c resp.Connection, cmdLine CmdLine) resp.Reply {
	name := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[name]
	if !ok {
		return reply.MakeStandardErrReply("ERR unknown command: " + name)
	}
	if !validArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(name)
	}
	fun := cmd.exector
	return fun(db, cmdLine[1:])
}

func validArity(arity int, cmdArgs [][]byte) bool {
	return arity == len(cmdArgs)
}

func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	val, ok := db.data.Get(key)
	return &database.DataEntity{
		Data: val,
	}, ok
}

func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity.Data)
}

func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity.Data)
}

func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity.Data)
}

func (db *DB) Remove(key string) {
	db.data.Remove(key)
}

func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, ok := db.data.Get(key)
		if ok {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

func (db *DB) Flush() {
	db.data.Clear()
}
