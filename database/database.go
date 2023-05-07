package database

import (
	"go-redis/aof"
	"go-redis/config"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"strconv"
	"strings"
)

type Database struct {
	dbSet      []*DB
	aofHandler *aof.AofHandler
}

func NewDatabase() *Database {
	database := &Database{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	database.dbSet = make([]*DB, config.Properties.Databases)
	for i := range database.dbSet {
		db := makeDB()
		db.index = i
		database.dbSet[i] = db
	}
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAofHandler(database)
		if err != nil {
			panic(err)
		}
		database.aofHandler = aofHandler
		for _, db := range database.dbSet {
			dbIndex := db.index
			db.addAof = func(line CmdLine) {
				database.aofHandler.AddAof(dbIndex, line)
			}
		}
	}
	return database
}

func (db *Database) Exec(c resp.Connection, args [][]byte) resp.Reply {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()
	cmdName := strings.ToLower(string(args[0]))
	if cmdName == "select" {
		if len(args) != 2 {
			return reply.MakeArgNumErrReply("select")
		}
		return execSelect(c, db, args[1:])
	}
	dbIndex := c.GetDBIndex()
	curDb := db.dbSet[dbIndex]
	return curDb.Exec(c, args)
}

func (db *Database) Close() {

}

func (db *Database) AfterClientClose(c resp.Connection) {

}

// select 1
func execSelect(c resp.Connection, database *Database, args [][]byte) resp.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeStandardErrReply("ERR invalid db index")
	}
	if dbIndex >= len(database.dbSet) {
		return reply.MakeStandardErrReply("ERR invalid db index")
	}
	c.SelectDB(dbIndex)
	return reply.MakeOkReply()
}
